SELECT launch_date
FROM products
WHERE category NOT IN ('Sports', 'Grocery', 'Fashion')
INTERSECT
SELECT opened_date AS launch_date
FROM accounts
WHERE status != 'CLOSED'
