SELECT QUARTER(ss.sale_date) AS sale_quarter, DATE_FORMAT(ss.sale_date, 'yyyy-MM') AS sale_month, AVG(i.quantity) AS avg_quantity
FROM inventory i
JOIN store_sales ss ON i.store_id = ss.store_id AND i.product_id = ss.product_id
WHERE i.quantity > 103
GROUP BY QUARTER(ss.sale_date), DATE_FORMAT(ss.sale_date, 'yyyy-MM')
