SELECT * FROM (SELECT merchant, txn_type, amount FROM transactions) src PIVOT (MAX(amount) AS total FOR txn_type IN ('DEBIT' AS debit, 'REFUND' AS refund))
