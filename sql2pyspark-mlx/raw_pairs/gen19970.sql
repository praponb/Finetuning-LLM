SELECT
    p.brand,
    DATE_FORMAT(p.launch_date, 'yyyy-MM') AS launch_month,
    ROUND(AVG(ss.revenue), 2) AS avg_revenue,
    ROUND(STDDEV(ss.units), 2) AS stddev_units
FROM store_sales ss
INNER JOIN stores s ON ss.store_id = s.id
INNER JOIN products p ON ss.product_id = p.id
GROUP BY p.brand, DATE_FORMAT(p.launch_date, 'yyyy-MM');
