WITH filtered AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        order_id,
        method,
        amount,
        paid_at,
        status,
        COALESCE(method, 'N/A') AS method_filled
    FROM payments
    WHERE order_id IS NOT NULL
      AND paid_at < '2024-06-04'
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        SUBSTRING(status, 1, 3) AS status_prefix,
        COALESCE(method, 'UNKNOWN') AS resolved_method
    FROM filtered
    WHERE method IS NOT NULL
),
flagged AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(paid_at) OVER (PARTITION BY order_id ORDER BY paid_at, id) IS NULL
                 OR paid_at > LAG(paid_at) OVER (PARTITION BY order_id ORDER BY paid_at, id) + INTERVAL 7 DAYS THEN 1 ELSE 0 END AS is_new_session
    FROM valid
),
islands AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_session) OVER (PARTITION BY order_id ORDER BY paid_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM flagged
),
with_dims AS (
    -- Attach orders attributes
    SELECT
        a.*,
        o.discount,
        o.status AS order_status,
        o.channel
    FROM islands a
    LEFT JOIN orders o
        ON a.order_id <=> o.id
),
grouped AS (
    -- Multi-level aggregation
    SELECT
        CASE WHEN amount >= 1500 THEN 'LARGE' ELSE 'SMALL' END AS amount_tier,
        DATE_TRUNC('MONTH', paid_at) AS activity_month,
        CASE WHEN discount >= 0.41 THEN 'PREMIUM' ELSE 'STANDARD' END AS discount_tier,
        COUNT(DISTINCT id) AS distinct_ids,
        COUNT_IF(order_status = 'PENDING') AS pending_events,
        APPROX_PERCENTILE(session_id, ARRAY(0.25, 0.5, 0.75)) AS session_id_quartiles
    FROM with_dims
    GROUP BY GROUPING SETS ((CASE WHEN amount >= 1500 THEN 'LARGE' ELSE 'SMALL' END, DATE_TRUNC('MONTH', paid_at), CASE WHEN discount >= 0.41 THEN 'PREMIUM' ELSE 'STANDARD' END), (CASE WHEN amount >= 1500 THEN 'LARGE' ELSE 'SMALL' END, DATE_TRUNC('MONTH', paid_at)), (CASE WHEN amount >= 1500 THEN 'LARGE' ELSE 'SMALL' END), ())
)
SELECT
    amount_tier,
    discount_tier,
    distinct_ids,
    pending_events,
    session_id_quartiles
FROM grouped
ORDER BY amount_tier NULLS FIRST;
