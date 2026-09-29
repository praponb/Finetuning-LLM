SELECT start_date
FROM subscriptions
WHERE customer_id IS NULL
UNION ALL
SELECT launch_date
FROM products
WHERE category NOT IN ('Grocery', 'Electronics')
ORDER BY start_date;
