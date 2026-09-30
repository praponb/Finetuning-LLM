SELECT customer_id, status
FROM accounts
WHERE account_type IN ('CHECKING', 'SAVINGS')
INTERSECT
SELECT customer_id, status
FROM accounts
WHERE customer_id IS NULL
