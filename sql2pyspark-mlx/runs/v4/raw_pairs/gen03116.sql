SELECT * FROM (SELECT HOUR(txn_time) AS txn_hour, txn_type, amount FROM transactions) PIVOT (SUM(amount) FOR txn_type IN ('FEE', 'REFUND', 'CREDIT')) ORDER BY txn_hour;
