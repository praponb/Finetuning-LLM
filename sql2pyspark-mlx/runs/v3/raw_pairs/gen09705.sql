SELECT * FROM (SELECT TO_DATE(txn_time) AS txn_day, txn_type, amount FROM transactions) PIVOT (COUNT(amount) FOR txn_type IN ('DEBIT', 'FEE', 'REFUND'));
