SELECT ss.store_id, QUARTER(ss.sale_date) AS sale_quarter, COUNT(DISTINCT i.quantity) AS unique_products
FROM inventory i
INNER JOIN store_sales ss ON i.store_id = ss.store_id AND i.product_id = ss.product_id
WHERE ss.revenue > 3700
GROUP BY ss.store_id, QUARTER(ss.sale_date)
