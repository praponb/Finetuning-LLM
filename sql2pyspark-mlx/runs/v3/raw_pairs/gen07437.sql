SELECT DATE_FORMAT(ss.sale_date, 'yyyy-MM') AS sale_month, COUNT(*) AS row_count, COUNT(i.quantity) AS quantity_count
FROM inventory i
LEFT JOIN store_sales ss ON i.store_id = ss.store_id AND i.product_id = ss.product_id
GROUP BY DATE_FORMAT(ss.sale_date, 'yyyy-MM')
ORDER BY sale_month
