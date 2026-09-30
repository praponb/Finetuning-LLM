SELECT MONTH(ss.sale_date) AS sale_month, AVG(i.quantity) AS avg_quantity, MAX(i.last_restock) AS latest_last_restock
FROM inventory i
LEFT JOIN store_sales ss ON i.store_id = ss.store_id AND i.product_id = ss.product_id
GROUP BY MONTH(ss.sale_date);
