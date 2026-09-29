SELECT customer_id
FROM accounts
WHERE balance BETWEEN 37000 AND 56000
INTERSECT
SELECT customer_id
FROM accounts
WHERE customer_id = 31
