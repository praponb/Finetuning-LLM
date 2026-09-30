SELECT * FROM (SELECT merchant, txn_type, amount FROM transactions WHERE merchant IS NOT NULL)
PIVOT (MAX(amount) FOR txn_type IN ('CREDIT', 'FEE', 'DEBIT', 'REFUND'));
