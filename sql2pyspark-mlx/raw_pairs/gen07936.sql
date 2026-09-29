SELECT
    o.status AS order_status,
    s.method,
    COUNT(*) AS rows_total,
    ROUND(SUM(s.amount), 2) AS total_amount,
    GROUPING_ID() AS grouping_level
FROM payments s
INNER JOIN orders o ON s.order_id = o.id
GROUP BY ROLLUP (o.status, s.method)
