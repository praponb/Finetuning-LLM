SELECT ss.id, ss.sale_date, ss.product_id
FROM store_sales ss
WHERE ss.units = 32 AND ss.product_id NOT IN (SELECT id FROM products WHERE MONTH(launch_date) IN (3, 9, 12))
ORDER BY ss.product_id DESC, id DESC
