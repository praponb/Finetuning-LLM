SELECT * FROM (SELECT txn_type, merchant, amount FROM transactions WHERE amount BETWEEN 1400 AND 3700)
PIVOT (AVG(amount) FOR merchant IN ('Lazada', '7-Eleven'))
