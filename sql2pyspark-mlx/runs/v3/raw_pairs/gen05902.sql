SELECT MONTH(ss.sale_date) AS sale_month, ss.product_id, AVG(i.quantity) AS avg_quantity
FROM inventory i
INNER JOIN store_sales ss ON i.store_id = ss.store_id AND i.product_id = ss.product_id
GROUP BY MONTH(ss.sale_date), ss.product_id
ORDER BY sale_month ASC, product_id ASC
LIMIT 10
