SELECT status, 'order' source
FROM orders
WHERE store_id IN (2, 5, 22, 32)
UNION ALL
SELECT status, 'account' source
FROM accounts
WHERE account_type IN ('CHECKING', 'LOAN', 'SAVINGS')
ORDER BY status;
