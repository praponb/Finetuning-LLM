SELECT ss.product_id, ss.units, p.launch_date
FROM store_sales ss
INNER JOIN products p ON ss.product_id = p.id
WHERE ss.store_id IN (2, 3)
