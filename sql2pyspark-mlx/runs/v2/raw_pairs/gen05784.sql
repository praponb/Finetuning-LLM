SELECT * FROM (SELECT merchant, txn_type, amount FROM transactions WHERE txn_time >= '2023-06-06')
PIVOT (AVG(amount) FOR txn_type IN ('DEBIT', 'CREDIT', 'REFUND', 'FEE'))
ORDER BY merchant
