SELECT * FROM (SELECT txn_type, merchant, amount FROM transactions)
PIVOT (COUNT(amount) FOR merchant IN ('Lazada', 'Shell', 'Grab', '7-Eleven'))
ORDER BY txn_type;
