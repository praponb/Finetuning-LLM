SELECT id, customer_id, opened_date
FROM accounts
WHERE customer_id = 19
INTERSECT
SELECT id, customer_id, opened_date
FROM accounts
WHERE customer_id IS NULL
