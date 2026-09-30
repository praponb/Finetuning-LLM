WITH filtered AS (
    SELECT
        *,
        ROUND(amount * 1.1, 2) AS amount_scaled,
        COALESCE(method, 'UNKNOWN') AS method_clean
    FROM payments
    WHERE paid_at >= TIMESTAMP '2023-08-27 20:00:00'
),
flagged AS (
    SELECT *, CASE WHEN paid_at - LAG(paid_at) OVER (PARTITION BY method ORDER BY paid_at ASC, id ASC) > INTERVAL '14' DAY THEN 1 ELSE 0 END AS is_new_burst
    FROM filtered
),
islands AS (
    SELECT
        id,
        order_id,
        method,
        paid_at,
        method_clean,
        SUM(is_new_burst) OVER (PARTITION BY method ORDER BY paid_at ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM flagged
)
SELECT
    DATE_TRUNC('MONTH', paid_at) AS activity_month,
    method,
    AVG(session_id) AS avg_session_id,
    COUNT_IF(method = 'cash') AS cash_events,
    GROUPING_ID() AS gid
FROM islands
GROUP BY CUBE (DATE_TRUNC('MONTH', paid_at), method)
HAVING COUNT_IF(method = 'cash') >= 1
ORDER BY method ASC
