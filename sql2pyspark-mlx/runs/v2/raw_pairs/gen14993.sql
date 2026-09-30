SELECT DATE_FORMAT(ss.sale_date, 'yyyy-MM') AS sale_month, MIN(i.last_restock) AS first_last_restock, COUNT(i.quantity) AS quantity_count
FROM inventory i
LEFT OUTER JOIN store_sales ss ON i.store_id = ss.store_id AND i.product_id = ss.product_id
GROUP BY DATE_FORMAT(ss.sale_date, 'yyyy-MM')
