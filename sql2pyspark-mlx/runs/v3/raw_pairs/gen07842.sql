SELECT launch_date
FROM products
INTERSECT
SELECT opened_date AS launch_date
FROM accounts
WHERE customer_id IS NULL;
