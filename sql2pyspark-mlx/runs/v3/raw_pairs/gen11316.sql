SELECT country
FROM customers
WHERE credit_limit > 6000
INTERSECT
SELECT country
FROM sessions
WHERE customer_id IN (37, 40);
