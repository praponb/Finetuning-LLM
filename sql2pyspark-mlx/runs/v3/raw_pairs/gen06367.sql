SELECT * FROM (SELECT YEAR(txn_time) AS txn_year, txn_type, amount FROM transactions WHERE is_flagged)
PIVOT (COUNT(amount) FOR txn_type IN ('CREDIT', 'REFUND', 'DEBIT', 'FEE'))
ORDER BY txn_year
