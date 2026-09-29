SELECT id, account_id
FROM transactions
WHERE merchant = 'Shell'
UNION ALL
SELECT id, account_id
FROM transactions
WHERE amount > 2800;
