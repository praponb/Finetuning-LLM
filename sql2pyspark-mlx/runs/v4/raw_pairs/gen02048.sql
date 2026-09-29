SELECT * FROM (SELECT txn_type, merchant, amount FROM transactions)
PIVOT (AVG(amount) FOR merchant IN ('Starbucks', 'Amazon', 'Lazada', 'Shell'))
