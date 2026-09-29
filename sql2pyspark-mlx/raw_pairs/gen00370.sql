WITH valid AS (
    -- Keep recent, valid rows
    SELECT
        *,
        COALESCE(amount, 0) AS amount_filled,
        COALESCE(method, 'DIRECT') AS resolved_method
    FROM payments
    WHERE method IS NOT NULL
      AND paid_at > DATE '2023-06-27'
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        COUNT(amount) OVER (PARTITION BY status ORDER BY paid_at, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS last3_count_amount,
        MIN(amount) OVER (PARTITION BY status ORDER BY paid_at, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS last3_min_amount,
        SUM(amount) OVER (PARTITION BY status ORDER BY paid_at, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS last3_sum_amount,
        SUM(amount_filled) OVER (PARTITION BY TO_DATE(paid_at), method ORDER BY amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_amount_filled
    FROM valid
    WHERE amount_filled < 42
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT
        *,
        SUBSTRING(method, 1, 4) AS method_prefix,
        COALESCE(method, 'UNKNOWN') AS resolved_method_2
    FROM enriched
    WHERE last3_sum_amount IS NOT NULL
      AND method IS NOT NULL
),
gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN paid_at - LAG(paid_at) OVER (PARTITION BY order_id ORDER BY paid_at, id) > INTERVAL 90 DAYS THEN 1 ELSE 0 END AS is_new_burst
    FROM cleaned
),
sessions_payments AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_burst) OVER (PARTITION BY order_id ORDER BY paid_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM gaps
),
metrics AS (
    -- Ranking and gap features
    SELECT
        *,
        MAX(is_new_burst) OVER w_hist AS running_max_is_new_burst,
        COUNT(is_new_burst) OVER w_hist AS running_count_is_new_burst,
        SUM(is_new_burst) OVER w_hist AS running_sum_is_new_burst,
        MIN(is_new_burst) OVER w_hist AS running_min_is_new_burst,
        LAST_VALUE(amount_filled, TRUE) OVER (PARTITION BY method, status ORDER BY amount_filled, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_amount_filled
    FROM sessions_payments
    WINDOW w_hist AS (PARTITION BY order_id ORDER BY paid_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
)
-- Aggregate with conditional metrics
SELECT
    DATE_TRUNC('YEAR', paid_at) AS year_start,
    CASE WHEN last3_count_amount >= 18 THEN 'HIGH' ELSE 'LOW' END AS last3_count_amount_tier,
    (UNIX_TIMESTAMP(MAX(paid_at)) - UNIX_TIMESTAMP(MIN(paid_at))) / 60 AS duration_min,
    SIZE(COLLECT_SET(method)) AS num_methods,
    SIZE(COLLECT_SET(method)) AS num_methods_2,
    APPROX_PERCENTILE(running_count_is_new_burst, 0.95) AS running_count_is_new_burst_p95,
    GROUPING_ID() AS gid
FROM metrics
GROUP BY GROUPING SETS ((DATE_TRUNC('YEAR', paid_at), CASE WHEN last3_count_amount >= 18 THEN 'HIGH' ELSE 'LOW' END), (DATE_TRUNC('YEAR', paid_at)))
ORDER BY num_methods DESC NULLS FIRST
