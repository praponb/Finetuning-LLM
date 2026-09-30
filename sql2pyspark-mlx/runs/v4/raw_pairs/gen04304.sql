WITH valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, CAST(amount AS INT) AS amount_int
    FROM payments
    WHERE method IS NOT NULL
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        order_id,
        method,
        amount,
        paid_at,
        status,
        amount_int,
        RANK() OVER (PARTITION BY method ORDER BY amount DESC) AS amount_rank,
        FIRST_VALUE(amount_int) IGNORE NULLS OVER (PARTITION BY order_id ORDER BY amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_amount_int,
        PERCENT_RANK() OVER (PARTITION BY status, order_id ORDER BY amount_int DESC) AS amount_int_percent_rank,
        MAX(amount_int) OVER (ORDER BY CAST(paid_at AS BIGINT) RANGE BETWEEN 2592000 PRECEDING AND CURRENT ROW) AS amount_int_max_30d,
        AVG(amount) OVER (PARTITION BY method ORDER BY amount_int, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS amount_avg_3
    FROM valid
),
joined AS (
    -- Attach orders attributes
    SELECT s.*, o.channel
    FROM enriched s
    LEFT JOIN orders o
        ON s.order_id = o.id
),
ranked AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *, ROW_NUMBER() OVER (PARTITION BY method, order_id ORDER BY paid_at DESC, id) AS row_num
    FROM joined
    QUALIFY row_num = 1
),
gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(paid_at) OVER (PARTITION BY channel ORDER BY paid_at, id) IS NULL
                 OR paid_at > LAG(paid_at) OVER (PARTITION BY channel ORDER BY paid_at, id) + INTERVAL 90 DAYS THEN 1 ELSE 0 END AS new_group_flag
    FROM ranked
),
islands AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(new_group_flag) OVER (PARTITION BY channel ORDER BY paid_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM gaps
),
ranked_payments AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY new_group_flag DESC NULLS LAST, id) AS rn
        FROM islands
    ) t
    WHERE rn = 1
)
-- Aggregate with conditional metrics
SELECT
    DATE_TRUNC('WEEK', paid_at) AS week_start,
    CASE WHEN amount >= 1600 THEN 'LARGE' ELSE 'SMALL' END AS amount_tier,
    MIN(paid_at) AS first_paid_at,
    PERCENTILE_APPROX(session_no, 0.5) AS session_no_p50,
    AVG(amount) FILTER (WHERE channel = 'mobile') AS mobile_avg_amount,
    MAX(amount_avg_3) FILTER (WHERE status = 'FAILED') AS failed_max_amount_avg_3,
    SIZE(COLLECT_SET(method)) AS num_methods,
    GROUPING_ID() AS grouping_id
FROM ranked_payments
GROUP BY GROUPING SETS ((DATE_TRUNC('WEEK', paid_at), CASE WHEN amount >= 1600 THEN 'LARGE' ELSE 'SMALL' END), (DATE_TRUNC('WEEK', paid_at)))
HAVING PERCENTILE_APPROX(session_no, 0.5) > 0;
