SELECT ss.id, ss.sale_date, ss.revenue
FROM store_sales ss
WHERE ss.product_id IN (2, 4, 5) AND ss.product_id IN (SELECT id FROM products WHERE price < 750)
ORDER BY ss.id
LIMIT 1;
