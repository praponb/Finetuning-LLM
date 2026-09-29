SELECT MONTH(ss.sale_date) AS sale_month, ROUND(AVG(i.quantity), 2) AS avg_quantity
FROM inventory i
LEFT JOIN store_sales ss ON i.store_id = ss.store_id AND i.product_id = ss.product_id
GROUP BY MONTH(ss.sale_date)
HAVING avg_quantity > 37;
