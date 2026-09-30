SELECT
    s.region,
    s.city,
    SUM(ss.revenue) AS total_revenue,
    AVG(ss.units) AS avg_units
FROM store_sales ss
RIGHT OUTER JOIN stores s ON ss.store_id = s.id
LEFT OUTER JOIN products p ON ss.product_id = p.id
WHERE p.product_name LIKE 'US%'
GROUP BY s.region, s.city
HAVING avg_units > 40
ORDER BY city, s.region
