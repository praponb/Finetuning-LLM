SELECT * FROM (SELECT is_flagged, txn_type, amount FROM transactions) PIVOT (SUM(amount) FOR txn_type IN ('REFUND', 'FEE')) ORDER BY is_flagged
