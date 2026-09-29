WITH gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(paid_at) OVER (PARTITION BY order_id ORDER BY paid_at, id) IS NULL
                 OR paid_at > LAG(paid_at) OVER (PARTITION BY order_id ORDER BY paid_at, id) + INTERVAL 90 DAYS THEN 1 ELSE 0 END AS session_start_flag
    FROM payments
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT
        id,
        order_id,
        amount,
        paid_at,
        status,
        SUM(session_start_flag) OVER (PARTITION BY order_id ORDER BY paid_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id
    FROM gaps
),
ranked AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM sessionized
    QUALIFY ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY island_id, id) = 1
),
latest AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM (
        SELECT *, RANK() OVER (PARTITION BY status, order_id ORDER BY paid_at DESC NULLS LAST) AS row_num
        FROM ranked
    ) ranked
    WHERE row_num = 1
),
ranked_payments AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY order_id, status ORDER BY paid_at DESC NULLS LAST, id) AS rn
        FROM latest
    ) t
    WHERE rn = 1
)
-- Aggregate with conditional metrics
SELECT
    DATE_TRUNC('WEEK', paid_at) AS week_start,
    CASE WHEN island_id >= 8 THEN 'LARGE' ELSE 'SMALL' END AS island_id_tier,
    APPROX_PERCENTILE(amount, 0.95) AS amount_p95,
    SIZE(COLLECT_SET(status)) AS num_status,
    COUNT(DISTINCT order_id) AS distinct_order_ids,
    MAX(island_id) FILTER (WHERE status = 'SUCCESS') AS success_max_island_id
FROM ranked_payments
GROUP BY DATE_TRUNC('WEEK', paid_at), CASE WHEN island_id >= 8 THEN 'LARGE' ELSE 'SMALL' END
ORDER BY success_max_island_id DESC
