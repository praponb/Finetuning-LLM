WITH metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        AVG(amount) OVER (PARTITION BY status ORDER BY amount, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS last7_avg_amount,
        MAX(amount) OVER (PARTITION BY status ORDER BY amount, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS last7_max_amount,
        SUM(amount) OVER (PARTITION BY status ORDER BY amount, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS last7_sum_amount,
        SUM(amount) OVER (ORDER BY paid_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_amount
    FROM payments
    WHERE order_id IS NULL
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        LEAD(amount, 2) OVER (PARTITION BY order_id ORDER BY cumulative_amount, id) AS next_amount,
        STDDEV_SAMP(last7_avg_amount) OVER (PARTITION BY DATE_TRUNC('month', paid_at), order_id) AS paid_at_period_stddev_samp_last7_avg_amount,
        NTILE(4) OVER w_part AS cumulative_amount_bucket,
        ROW_NUMBER() OVER (PARTITION BY status ORDER BY cumulative_amount DESC, id) AS row_seq,
        FIRST_VALUE(method, TRUE) OVER w_part_2 AS first_known_method
    FROM metrics
    WINDOW w_part AS (ORDER BY cumulative_amount, id), w_part_2 AS (ORDER BY last7_sum_amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
prev_events AS (
    -- Previous event time per key
    SELECT *, LAG(paid_at) OVER (PARTITION BY method ORDER BY paid_at, id) AS prev_paid_at
    FROM with_windows
),
flagged AS (
    -- Flag rows that start a new session
    SELECT *, CASE WHEN prev_paid_at IS NULL OR paid_at > prev_paid_at + INTERVAL '30' DAY THEN 1 ELSE 0 END AS is_new_burst
    FROM prev_events
),
sessions_payments AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_burst) OVER (PARTITION BY method ORDER BY paid_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM flagged
),
with_dims AS (
    -- Attach orders attributes
    SELECT
        s.id,
        s.order_id,
        s.method,
        s.amount,
        s.paid_at,
        s.last7_sum_amount,
        s.next_amount,
        s.cumulative_amount_bucket,
        s.first_known_method,
        s.prev_paid_at,
        s.is_new_burst,
        s.burst_id,
        d.order_date,
        d.status AS order_status
    FROM sessions_payments s
    LEFT JOIN orders d
        ON s.order_id <=> d.id
),
enriched AS (
    -- Attach orders attributes
    SELECT
        s.*,
        o.amount AS order_amount,
        o.order_date AS order_order_date,
        o.status
    FROM with_dims s
    LEFT JOIN orders o
        ON s.order_id = o.id
),
deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *, ROW_NUMBER() OVER (PARTITION BY method ORDER BY order_order_date, id) AS dedup_rank
    FROM enriched
    QUALIFY dedup_rank = 1
),
with_gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(paid_at) OVER (PARTITION BY order_status ORDER BY paid_at, id) IS NULL
                 OR paid_at > LAG(paid_at) OVER (PARTITION BY order_status ORDER BY paid_at, id) + INTERVAL 30 DAYS THEN 1 ELSE 0 END AS is_new_burst_2
    FROM deduped
),
sessions_payments_2 AS (
    -- Running sum of the flag = session number
    SELECT
        id,
        amount,
        paid_at,
        last7_sum_amount,
        first_known_method,
        burst_id,
        order_date,
        order_status,
        dedup_rank,
        SUM(is_new_burst_2) OVER (PARTITION BY order_status ORDER BY paid_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id_2
    FROM with_gaps
)
-- Grouped metrics
SELECT
    DATE_TRUNC('QUARTER', paid_at) AS quarter_start,
    ROUND(SUM(amount) / NULLIF(COUNT(CASE WHEN order_status = 'COMPLETED' THEN 1 END), 0), 2) AS amount_per_completed,
    COUNT(*) FILTER (WHERE order_status = 'SHIPPED') AS shipped_rows,
    APPROX_PERCENTILE(burst_id_2, 0.95) AS burst_id_2_p95
FROM sessions_payments_2
GROUP BY DATE_TRUNC('QUARTER', paid_at)
ORDER BY quarter_start NULLS FIRST
