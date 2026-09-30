SELECT * FROM (SELECT txn_type, merchant, amount FROM transactions)
PIVOT (SUM(amount) FOR merchant IN ('Shell', '7-Eleven'))
