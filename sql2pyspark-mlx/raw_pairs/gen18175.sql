SELECT ss.id, ss.store_id
FROM store_sales ss
WHERE ss.units IS NOT NULL AND ss.product_id NOT IN (SELECT id FROM products WHERE launch_date BETWEEN '2023-04-11' AND '2025-02-01')
