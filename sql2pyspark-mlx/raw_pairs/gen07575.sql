WITH by_dimension AS (
    SELECT
        d.status AS order_status,
        s.method,
        COUNT(*) AS rows_total,
        SUM(s.amount) AS total_amount
    FROM payments s
    LEFT JOIN orders d ON s.order_id = d.id
    GROUP BY GROUPING SETS ((d.status, s.method), (d.status))
)
SELECT
    order_status,
    method,
    rows_total,
    total_amount,
    CASE WHEN order_status RLIKE '^(SHIPPED|CANCELLED)$' THEN 'EXTENDED' ELSE 'GROUP_A' END AS order_status_zone
FROM by_dimension
