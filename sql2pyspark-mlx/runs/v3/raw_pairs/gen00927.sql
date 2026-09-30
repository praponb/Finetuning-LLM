SELECT * FROM (SELECT txn_type, merchant, amount FROM transactions) PIVOT (AVG(amount) FOR merchant IN ('Lazada', 'Starbucks', 'Amazon')) ORDER BY txn_type;
