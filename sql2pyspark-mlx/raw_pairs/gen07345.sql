SELECT
    dim.status AS order_status,
    a.status AS payment_status,
    COUNT(*) AS rows_total,
    ROUND(AVG(a.amount), 2) AS avg_amount,
    GROUPING_ID() AS level_id
FROM payments a
LEFT JOIN orders dim ON a.order_id = dim.id
GROUP BY GROUPING SETS ((dim.status, a.status), (dim.status))
ORDER BY order_status DESC NULLS FIRST
