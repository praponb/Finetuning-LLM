SELECT customer_id
FROM accounts
WHERE balance BETWEEN 70000 AND 75000
INTERSECT
SELECT customer_id
FROM accounts
WHERE customer_id = 12;
