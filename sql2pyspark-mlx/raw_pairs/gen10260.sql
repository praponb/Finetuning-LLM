WITH latest AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM payments
    QUALIFY ROW_NUMBER() OVER (PARTITION BY method, status ORDER BY amount DESC NULLS LAST, id) = 1
),
gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(paid_at) OVER (PARTITION BY order_id ORDER BY paid_at, id) IS NULL
                 OR paid_at > LAG(paid_at) OVER (PARTITION BY order_id ORDER BY paid_at, id) + INTERVAL '7' DAY THEN 1 ELSE 0 END AS is_new_burst
    FROM latest
),
islands AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_burst) OVER (PARTITION BY order_id ORDER BY paid_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM gaps
),
metrics AS (
    -- Ranking and gap features
    SELECT
        *,
        SUM(amount) OVER (PARTITION BY order_id, status ORDER BY session_no, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) last7_sum_amount,
        MAX(amount) OVER (PARTITION BY order_id, status ORDER BY session_no, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) last7_max_amount,
        AVG(amount) OVER (PARTITION BY order_id, status ORDER BY session_no, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) last7_avg_amount
    FROM islands
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        LAST_VALUE(last7_sum_amount) IGNORE NULLS OVER w_part last_known_last7_sum_amount,
        SUM(last7_avg_amount) OVER (PARTITION BY order_id) order_id_sum_last7_avg_amount,
        NTILE(5) OVER w_recent is_new_burst_bucket
    FROM metrics
    WINDOW w_part AS (PARTITION BY TO_DATE(paid_at), status ORDER BY paid_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), w_recent AS (PARTITION BY order_id ORDER BY is_new_burst DESC, id)
),
with_prev AS (
    -- Previous event time per key
    SELECT *, LAG(paid_at) OVER (PARTITION BY method ORDER BY paid_at, id) AS prev_paid_at
    FROM with_windows
),
flagged AS (
    -- Flag rows that start a new session
    SELECT *, CASE WHEN prev_paid_at IS NULL OR paid_at > prev_paid_at + INTERVAL 60 DAYS THEN 1 ELSE 0 END AS session_start_flag
    FROM with_prev
),
islands_payments AS (
    -- Running sum of the flag = session number
    SELECT
        id,
        method,
        amount,
        paid_at,
        status,
        is_new_burst,
        last7_sum_amount,
        last_known_last7_sum_amount,
        order_id_sum_last7_avg_amount,
        SUM(session_start_flag) OVER (PARTITION BY method ORDER BY paid_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM flagged
),
kept_keys AS (
    -- Set difference on keys
    SELECT id
    FROM islands_payments
    EXCEPT DISTINCT
    SELECT id
    FROM payments
    WHERE amount > 990
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM islands_payments s
    JOIN kept_keys k ON s.id = k.id
),
totals AS (
    -- Multi-level aggregation
    SELECT
        CASE WHEN last7_sum_amount >= 9000 THEN 'HIGH' ELSE 'LOW' END AS last7_sum_amount_tier,
        DATE_TRUNC('QUARTER', paid_at) AS quarter_start,
        COUNT(DISTINCT id) AS distinct_ids,
        ROUND(AVG(order_id_sum_last7_avg_amount), 2) AS avg_order_id_sum_last7_avg_amount,
        ROUND(SUM(last7_sum_amount) / NULLIF(COUNT(CASE WHEN status = 'FAILED' THEN 1 END), 0), 2) AS last7_sum_amount_per_failed,
        MIN(paid_at) AS started_at,
        SIZE(COLLECT_SET(method)) AS num_methods,
        SORT_ARRAY(COLLECT_SET(method)) AS method_list
    FROM reconciled
    GROUP BY CASE WHEN last7_sum_amount >= 9000 THEN 'HIGH' ELSE 'LOW' END, DATE_TRUNC('QUARTER', paid_at)
)
SELECT
    last7_sum_amount_tier,
    quarter_start,
    avg_order_id_sum_last7_avg_amount,
    started_at,
    num_methods,
    method_list
FROM totals
ORDER BY num_methods DESC NULLS LAST;
