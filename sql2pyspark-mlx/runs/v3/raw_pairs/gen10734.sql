SELECT ss.id, ss.revenue, ss.units
FROM store_sales ss
WHERE ss.product_id NOT IN (SELECT id FROM products WHERE launch_date < '2023-09-01')
