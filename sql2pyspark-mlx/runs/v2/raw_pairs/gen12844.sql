SELECT
    s.city,
    MONTH(s.opened_date) AS opened_month,
    SUM(CASE WHEN o.channel = 'web' THEN 1 ELSE 0 END) AS web_count,
    COUNT(*) AS row_count
FROM orders o
INNER JOIN customers c ON o.customer_id = c.id
RIGHT OUTER JOIN stores s ON o.store_id = s.id
GROUP BY s.city, MONTH(s.opened_date);
