SELECT
    DATE_TRUNC('MONTH', p.launch_date) AS launch_month,
    YEAR(p.launch_date) AS launch_year,
    COUNT(oi.unit_price) AS unit_price_count,
    COUNT(*) AS cnt
FROM order_items oi
INNER JOIN products p ON p.id = oi.product_id
GROUP BY DATE_TRUNC('MONTH', p.launch_date), YEAR(p.launch_date)
ORDER BY launch_month;
