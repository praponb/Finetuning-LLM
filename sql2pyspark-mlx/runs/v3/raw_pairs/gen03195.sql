SELECT * FROM (SELECT txn_type, merchant, amount FROM transactions WHERE TO_DATE(txn_time) >= '2025-02-16')
PIVOT (AVG(amount) FOR merchant IN ('Grab', '7-Eleven', 'Starbucks', 'Amazon'))
ORDER BY txn_type;
