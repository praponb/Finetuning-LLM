SELECT * FROM (SELECT txn_type, merchant, amount FROM transactions) PIVOT (MAX(amount) FOR merchant IN ('Lazada', '7-Eleven')) ORDER BY txn_type
