SELECT * FROM (SELECT merchant, txn_type, amount FROM transactions) PIVOT (SUM(amount) FOR txn_type IN ('FEE', 'REFUND'))
