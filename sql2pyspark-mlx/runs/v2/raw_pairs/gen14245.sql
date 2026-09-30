SELECT * FROM (SELECT TO_DATE(txn_time) AS txn_day, txn_type, amount FROM transactions) PIVOT (SUM(amount) FOR txn_type IN ('FEE', 'REFUND', 'CREDIT', 'DEBIT'));
