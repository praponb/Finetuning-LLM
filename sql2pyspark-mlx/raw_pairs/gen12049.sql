WITH base AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        order_id,
        method,
        amount,
        paid_at,
        FLOOR(amount) AS amount_whole,
        CASE WHEN amount >= 1270 THEN 'High' WHEN amount >= 210 THEN 'Medium' ELSE 'Low' END AS amount_band,
        COALESCE(method, 'UNKNOWN') AS resolved_method
    FROM payments
    WHERE paid_at > TIMESTAMP '2023-08-13 19:00:00'
      AND order_id IS NOT NULL
),
ranked AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY method, order_id ORDER BY amount_whole DESC, id) AS latest_rank
        FROM base
    ) x
    WHERE latest_rank = 1
),
enriched AS (
    -- Attach orders attributes
    SELECT
        a.*,
        d.status,
        d.amount AS order_amount
    FROM ranked a
    LEFT JOIN orders d
        ON a.order_id <=> d.id
),
with_gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN paid_at - LAG(paid_at) OVER (PARTITION BY method ORDER BY paid_at, id) > INTERVAL 90 DAYS THEN 1 ELSE 0 END AS is_new_session
    FROM enriched
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_session) OVER (PARTITION BY method ORDER BY paid_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM with_gaps
),
with_dims AS (
    -- Attach orders attributes
    SELECT
        src.*,
        p.status AS order_status,
        p.discount
    FROM sessionized src
    LEFT JOIN orders p
        ON src.order_id = p.id
)
-- Grouped metrics
SELECT
    DATE_TRUNC('YEAR', paid_at) AS year_start,
    CASE WHEN amount_whole >= 85 THEN 'PREMIUM' ELSE 'STANDARD' END AS amount_whole_tier,
    COUNT(CASE WHEN status = 'SHIPPED' THEN 1 END) AS shipped_count,
    COUNT(CASE WHEN method = 'cash' THEN 1 END) AS cash_count,
    MIN(paid_at) AS first_paid_at,
    PERCENTILE_APPROX(session_id, 0.95) AS session_id_p95
FROM with_dims
GROUP BY GROUPING SETS ((DATE_TRUNC('YEAR', paid_at), CASE WHEN amount_whole >= 85 THEN 'PREMIUM' ELSE 'STANDARD' END), (DATE_TRUNC('YEAR', paid_at)))
HAVING PERCENTILE_APPROX(session_id, 0.95) > 0
