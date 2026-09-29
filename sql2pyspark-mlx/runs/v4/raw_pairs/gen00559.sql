WITH gaps AS (
    SELECT *, CASE WHEN LAG(paid_at) OVER (PARTITION BY method ORDER BY paid_at, id) IS NULL
                 OR paid_at > LAG(paid_at) OVER (PARTITION BY method ORDER BY paid_at, id) + INTERVAL '60' DAY THEN 1 ELSE 0 END AS new_group_flag
    FROM payments
),
sessionized AS (
    SELECT *, SUM(new_group_flag) OVER (PARTITION BY method ORDER BY paid_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id
    FROM gaps
)
SELECT
    id,
    paid_at,
    status,
    new_group_flag,
    island_id
FROM sessionized
ORDER BY paid_at DESC
