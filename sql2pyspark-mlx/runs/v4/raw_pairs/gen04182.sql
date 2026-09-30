WITH filtered AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        CASE WHEN method = 'promptpay' THEN 1 ELSE 0 END is_promptpay,
        SUBSTRING(status, 1, 3) status_prefix
    FROM payments
    WHERE method IS NOT NULL
),
gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(paid_at) OVER (PARTITION BY status ORDER BY paid_at, id) IS NULL
                 OR paid_at > LAG(paid_at) OVER (PARTITION BY status ORDER BY paid_at, id) + INTERVAL '60' DAY THEN 1 ELSE 0 END AS is_new_session
    FROM filtered
),
islands AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_session) OVER (PARTITION BY status ORDER BY paid_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM gaps
),
with_dims AS (
    -- Attach orders attributes
    SELECT src.*, p.amount AS order_amount
    FROM islands src
    LEFT JOIN orders p
        ON src.order_id = p.id
),
enriched AS (
    -- Attach orders attributes
    SELECT
        e.*,
        o.shipping_fee,
        o.order_date,
        o.discount
    FROM with_dims e
    LEFT JOIN orders o
        ON e.order_id = o.id
)
-- Grouped metrics
SELECT
    DATE_TRUNC('YEAR', paid_at) AS year_start,
    CASE WHEN is_new_session >= 1 THEN 'PREMIUM' ELSE 'STANDARD' END AS is_new_session_tier,
    method,
    MAX(paid_at) AS last_seen,
    COUNT(DISTINCT method) AS distinct_methods,
    COUNT(*) FILTER (WHERE status = 'SUCCESS') AS success_rows,
    PERCENTILE_APPROX(is_new_session, 0.9) AS is_new_session_p90,
    DENSE_RANK() OVER (PARTITION BY DATE_TRUNC('YEAR', paid_at) ORDER BY COUNT(DISTINCT method) DESC) AS distinct_methods_dense_rank
FROM enriched
GROUP BY DATE_TRUNC('YEAR', paid_at), CASE WHEN is_new_session >= 1 THEN 'PREMIUM' ELSE 'STANDARD' END, method
