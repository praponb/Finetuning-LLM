SELECT launch_date AS event_date
FROM products
WHERE launch_date IS NOT NULL
EXCEPT
SELECT sale_date AS event_date
FROM store_sales
WHERE sale_date IS NOT NULL
ORDER BY event_date;
