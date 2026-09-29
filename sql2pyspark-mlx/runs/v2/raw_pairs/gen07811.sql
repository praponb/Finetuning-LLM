SELECT ss.id, ss.sale_date
FROM store_sales ss
WHERE ss.product_id IN (SELECT id FROM products WHERE brand = 'Umbrella')
ORDER BY id, sale_date DESC
