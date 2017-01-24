'''
新生銀行、三井住友銀行、三菱東京UFJ銀行の入出金明細CSVをマージする。
input/shinsei, mitsui, mitsubishiにCSVをそれぞれ配置する。
メモ：
  日付が前後するケースがあったのでsdt(ソート用日付)を設けた。
  期間に抜けや重複があっても残高は狂わないようにした。
  各銀行明細の最初のレコードとして「初期残高」行を追加する
'''
import glob
import pandas as pd
from datetime import datetime
from datetime import timedelta

COMMON_COLUMNS = ['dt', 'summary', 'payment', 'deposit_amt', 'balance', 'bank', 'sdt']
def init_balance_sum(df):
	init_balance_sum = df['balance'][0] - df['deposit_amt'][0] + df['payment'][0]
	init_balance_row = pd.DataFrame()
	init_balance_row = init_balance_row.append({
		'dt': df['dt'][0] - timedelta(days=1),
		'summary': '初期残高',
		'payment': 0,
		'deposit_amt': init_balance_sum,
		'balance': init_balance_sum,
		'bank': df['bank'][0]
	}, ignore_index=True)
	return pd.concat([init_balance_row, df])

def sdt(df):
	prev_dt = datetime.strptime('1980/01/01', '%Y/%m/%d')
	sdt_list = []
	for i, seri in df.iterrows():
		if prev_dt > seri['dt']:
			sdt_list.append(prev_dt)
		else:
			prev_dt = seri['dt']
			sdt_list.append(seri['dt'])
	df['sdt'] = sdt_list
	return df

def read_shinsei():
	pathes = glob.glob("./input/shinsei/*.csv")
	COLUMNS = ['dt_str', 'ref_num', 'summary', 'payment', 'deposit_amt', 'balance']
	df = pd.DataFrame()
	for path in pathes:
		print(path)
		# なぜかread_csvで１行しか読めない(skiprowsもできない)
		# df = pd.read_csv(path, sep="\t", skiprows=10, encoding='utf_16')
		# readで読んで配列からデータフレームを作成する
		with open(path, encoding='utf_16') as f:
			fstr = f.read()
		data = list(map(lambda s: s.split('\t'), fstr.split('\n')))
		shinseis = list(filter(lambda l: len(l) == 6, data))
		r_df = pd.DataFrame(shinseis[1:], columns=COLUMNS)
		if len(r_df) != len(r_df.drop_duplicates(COLUMNS)):
			print('WARNING:本スクリプトが想定しない同一ファイル内の同一値レコードが見つかりました.')
		df = pd.concat([df, r_df])
	df = df.drop_duplicates(COLUMNS)
	df['dt'] = df['dt_str'].apply(lambda s: datetime.strptime(s, '%Y/%m/%d'))
	df['bank'] = '新生'
	# 数字を数値に
	df['payment'] = df['payment'].apply(lambda s: 0 if s == '' else int(s))
	df['deposit_amt'] = df['deposit_amt'].apply(lambda s: 0 if s == '' else int(s))
	df['balance'] = df['balance'].apply(lambda s: 0 if s == '' else int(s))
	df = df.sort_index(ascending=False).reset_index(drop=True)  # 新生は降順
	df = init_balance_sum(df)
	df = sdt(df)
	return df[COMMON_COLUMNS]
df_shinsei = read_shinsei()

def read_mitsubishi():
	pathes = glob.glob("./input/mitsubishi/*.csv")

	df = pd.DataFrame()
	COLUMNS = ['dt_str', 'summary', 'summary_contents', 'payment', 'deposit_amt', 'balance', 'memo', '未資金化区分', '入払区分']
	for path in pathes:
		print(path)
		r_df = pd.read_csv(path, encoding='cp932')
		r_df.columns = COLUMNS
		if len(r_df) != len(r_df.drop_duplicates(COLUMNS)):
			print('WARNING:本スクリプトが想定しない同一ファイル内の同一値レコードが見つかりました.')
		df = pd.concat([df, r_df])

	df = df.drop_duplicates(COLUMNS)
	df['dt'] = df['dt_str'].apply(lambda s: datetime.strptime(s, '%Y/%m/%d'))
	df['bank'] = '三菱東京UFJ'
	df['payment'] = df['payment'].apply(lambda v: int(v.replace(',', '')) if isinstance(v, str) else v)
	df['deposit_amt'] = df['deposit_amt'].apply(lambda v: int(v.replace(',', '')) if isinstance(v, str) else v)
	df['balance'] = df['balance'].apply(lambda v: int(v.replace(',', '')) if isinstance(v, str) else v)
	df = df.fillna(0)
	df = init_balance_sum(df)
	df = sdt(df)
	return df[COMMON_COLUMNS]

df_mitsubishi = read_mitsubishi()


def read_mitsui():
	pathes = glob.glob("./input/mitsui/*.csv")
	COLUMNS = ['dt_jp', 'payment', 'deposit_amt', 'summary', 'balance']
	df = pd.DataFrame()
	for path in pathes:
		print(path)
		r_df = pd.read_csv(path, encoding='cp932')
		r_df.columns = COLUMNS
		if len(r_df) != len(r_df.drop_duplicates(COLUMNS)):
			print('WARNING:本スクリプトが想定しない同一ファイル内の同一値レコードが見つかりました.')
		df = pd.concat([df, r_df])

	df = df.drop_duplicates(['dt_jp', 'payment', 'deposit_amt', 'summary', 'balance'])

	# 和暦を西暦に変換
	def dt_AD(s):
		if s[0] == 'H':
			return "20%02d%s" % (int(s[1:3]) - 12, s[3:])
		else:
			return "error"
	df['dt_str'] = df['dt_jp'].apply(dt_AD)
	df['dt'] = df['dt_str'].apply(lambda s: datetime.strptime(s, '%Y.%m.%d'))
	df['bank'] = '三井住友'
	df = df.fillna(0)
	df = init_balance_sum(df)
	df = sdt(df)
	return df[COMMON_COLUMNS]

df_mitsui = read_mitsui()

df_all = pd.concat([df_shinsei.reset_index(), df_mitsui.reset_index(), df_mitsubishi.reset_index()])
df_all = df_all.sort_values(by=['sdt', 'bank', 'index']).reset_index(drop=True)
del df_all['sdt']
del df_all['index']

# 残高合計の算出
deposit_sum = 0
balance_mitsui = 0
balance_mitsubishi = 0
balance_shinsei = 0
balance_sum = 0
balance_sums = []
for i, seri in df_all.iterrows():
	deposit_sum = deposit_sum + seri['deposit_amt'] - seri['payment']
	if seri['bank'] == '三井住友': balance_mitsui = seri['balance']
	elif seri['bank'] == '三菱東京UFJ': balance_mitsubishi = seri['balance']
	elif seri['bank'] == '新生': balance_shinsei = seri['balance']
	balance_sum = balance_mitsubishi + balance_shinsei + balance_mitsui
	if deposit_sum != balance_sum:
		print('WARNING:残高と預入＋支払計が合いません.データ期間に抜けがありそうです.')
		print(seri)
	balance_sums.append(balance_sum)
df_all['balance_sum'] = balance_sums
df_all.to_csv('output/統合入出金明細.csv')

# 月ごとの最大金額
df_all_mon = df_all.copy()
df_all_mon['mon'] = df_all_mon['dt'].apply(lambda d: datetime.strftime(d, '%Y/%m'))
df_all_mon.groupby('mon')['balance_sum'].max().reset_index(name='balance_sum').to_csv('output/月別最大残高合計.csv')
