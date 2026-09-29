SELECT merchant
FROM transactions
WHERE account_id IN (1, 7, 14)
UNION
SELECT merchant
FROM transactions
WHERE txn_type = 'REFUND'
