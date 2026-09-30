SELECT
    ss.store_id,
    DATE_FORMAT(ss.sale_date, 'yyyy-MM') AS sale_month,
    COUNT(*) AS cnt,
    ROUND(AVG(i.quantity), 2) AS avg_quantity
FROM inventory i
INNER JOIN store_sales ss ON i.store_id = ss.store_id AND i.product_id = ss.product_id
GROUP BY ss.store_id, DATE_FORMAT(ss.sale_date, 'yyyy-MM');
