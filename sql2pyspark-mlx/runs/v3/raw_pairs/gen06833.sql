SELECT opened_date
FROM accounts
WHERE customer_id IN (21, 32)
UNION ALL
SELECT opened_date
FROM accounts
WHERE account_type = 'CHECKING'
ORDER BY opened_date
