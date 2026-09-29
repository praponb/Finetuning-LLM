WITH with_gaps AS (
    SELECT *, CASE WHEN LAG(paid_at) OVER (PARTITION BY status ORDER BY paid_at, id) IS NULL
                 OR paid_at > LAG(paid_at) OVER (PARTITION BY status ORDER BY paid_at, id) + INTERVAL 14 DAYS THEN 1 ELSE 0 END AS new_group_flag
    FROM payments
)
SELECT
    id,
    order_id,
    paid_at,
    status,
    SUM(new_group_flag) OVER (PARTITION BY status ORDER BY paid_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id
FROM with_gaps
