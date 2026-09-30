SELECT ss.id, ss.units, ss.revenue
FROM store_sales ss
WHERE ss.product_id NOT IN (SELECT id FROM products WHERE product_name LIKE '%Runni%')
