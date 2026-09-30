WITH base AS (
    SELECT *, COALESCE(method, 'Unknown') AS method_filled
    FROM payments
    WHERE paid_at >= TIMESTAMP '2023-06-03 19:00:00'
      AND order_id IN (2, 8, 12, 36)
),
gaps AS (
    SELECT *, CASE WHEN paid_at - LAG(paid_at) OVER (PARTITION BY method ORDER BY paid_at, id) > INTERVAL '30' DAY THEN 1 ELSE 0 END AS new_group_flag
    FROM base
),
islands AS (
    SELECT
        id,
        order_id,
        method,
        paid_at,
        status,
        method_filled,
        SUM(new_group_flag) OVER (PARTITION BY method ORDER BY paid_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM gaps
)
SELECT
    CASE WHEN session_id >= 7 THEN 'LARGE' ELSE 'SMALL' END AS session_id_tier,
    COUNT_IF(status = 'REFUNDED') AS refunded_events,
    MAX(session_id) FILTER (WHERE method = 'paypal') AS paypal_max_session_id,
    APPROX_PERCENTILE(session_id, 0.95) AS session_id_p95,
    APPROX_PERCENTILE(session_id, 0.95) AS session_id_p95_2,
    GROUPING(CASE WHEN session_id >= 7 THEN 'LARGE' ELSE 'SMALL' END) AS grouping_level
FROM islands
GROUP BY ROLLUP (CASE WHEN session_id >= 7 THEN 'LARGE' ELSE 'SMALL' END)
HAVING COUNT_IF(status = 'REFUNDED') < 1000
ORDER BY session_id_tier DESC;
