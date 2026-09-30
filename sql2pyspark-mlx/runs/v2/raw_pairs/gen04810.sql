SELECT ss.id, ss.sale_date, ss.product_id
FROM store_sales ss
WHERE ss.product_id IN (SELECT id FROM products WHERE price <= 950)
