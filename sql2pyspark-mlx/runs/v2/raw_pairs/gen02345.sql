SELECT * FROM (SELECT txn_type, merchant, amount FROM transactions WHERE account_id IN (11, 17, 39)) PIVOT (MAX(amount) FOR merchant IN ('Lazada', 'Starbucks', 'Shell')) ORDER BY txn_type
