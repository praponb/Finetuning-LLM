SELECT DATE_FORMAT(ss.sale_date, 'yyyy-MM') AS sale_month, MIN(i.last_restock) AS first_last_restock, SUM(i.quantity) AS total_quantity
FROM inventory i
JOIN store_sales ss ON i.store_id = ss.store_id AND i.product_id = ss.product_id
GROUP BY DATE_FORMAT(ss.sale_date, 'yyyy-MM');
