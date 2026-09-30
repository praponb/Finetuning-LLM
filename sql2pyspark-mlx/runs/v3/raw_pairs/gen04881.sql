SELECT * FROM (SELECT txn_type, merchant, amount FROM transactions) PIVOT (SUM(amount) FOR merchant IN ('Grab', 'Shell', '7-Eleven')) ORDER BY txn_type
