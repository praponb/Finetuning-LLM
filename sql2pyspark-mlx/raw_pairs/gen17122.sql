WITH filtered AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        order_id,
        paid_at,
        status,
        ROUND(amount * 1.15, 2) amount_scaled
    FROM payments
    WHERE HOUR(paid_at) BETWEEN 11 AND 19
),
gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN paid_at - LAG(paid_at) OVER (PARTITION BY status ORDER BY paid_at, id) > INTERVAL 30 DAYS THEN 1 ELSE 0 END AS session_start_flag
    FROM filtered
),
islands AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(session_start_flag) OVER (PARTITION BY status ORDER BY paid_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM gaps
)
-- Aggregate with conditional metrics
SELECT
    DATE_TRUNC('YEAR', paid_at) AS year_start,
    COUNT(DISTINCT id) AS distinct_ids,
    SORT_ARRAY(COLLECT_SET(status)) AS status_list,
    COUNT(*) AS event_count
FROM islands
GROUP BY year_start
HAVING COUNT(*) < 1000
ORDER BY year_start DESC NULLS FIRST;
