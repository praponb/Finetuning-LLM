SELECT * FROM (SELECT merchant, txn_type, amount FROM transactions)
PIVOT (MAX(amount) FOR txn_type IN ('DEBIT', 'REFUND'));
