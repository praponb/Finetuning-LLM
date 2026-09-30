SELECT *
FROM transactions
WHERE amount <= 2500 AND (amount > 1200 OR txn_type IN ('FEE', 'CREDIT'));
