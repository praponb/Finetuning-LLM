SELECT * FROM (SELECT merchant, txn_type, amount FROM transactions)
PIVOT (AVG(amount) FOR txn_type IN ('FEE', 'CREDIT'));
