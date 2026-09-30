SELECT
    o.status AS order_status,
    a.method,
    COUNT(*) AS num_rows,
    AVG(a.amount) AS avg_amount
FROM payments a
INNER JOIN orders o ON a.order_id = o.id
GROUP BY GROUPING SETS ((o.status, a.method), (o.status), ())
ORDER BY method DESC NULLS LAST
