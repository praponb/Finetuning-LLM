SELECT country
FROM customers
WHERE signup_date < '2023-10-07'
INTERSECT
SELECT country
FROM sessions
WHERE customer_id IN (27, 36);
