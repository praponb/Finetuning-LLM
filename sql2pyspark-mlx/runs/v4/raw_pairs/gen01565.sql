SELECT id, customer_id, status
FROM accounts
WHERE account_type IN ('SAVINGS', 'CHECKING', 'LOAN')
UNION ALL
SELECT id, customer_id, status
FROM accounts
WHERE YEAR(opened_date) = 2024
ORDER BY status
