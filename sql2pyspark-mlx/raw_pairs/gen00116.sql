SELECT
    ss.product_id,
    DATE_FORMAT(ss.sale_date, 'yyyy-MM') AS sale_month,
    SUM(i.quantity) AS total_quantity,
    MAX(i.quantity) AS max_quantity
FROM inventory i
LEFT JOIN store_sales ss ON i.store_id = ss.store_id AND i.product_id = ss.product_id
WHERE ss.product_id IN (1, 6)
GROUP BY ss.product_id, DATE_FORMAT(ss.sale_date, 'yyyy-MM')
