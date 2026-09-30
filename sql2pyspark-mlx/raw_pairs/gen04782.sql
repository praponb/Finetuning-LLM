WITH with_windows AS (
    SELECT
        *,
        RANK() OVER (ORDER BY amount DESC) amount_rank,
        ROUND(amount / SUM(amount) OVER (PARTITION BY order_id, method), 4) amount_share,
        LAST_VALUE(method, TRUE) OVER w_part last_known_method,
        AVG(amount) OVER (PARTITION BY TO_DATE(paid_at), method) paid_at_to_date_avg_amount,
        SUM(amount) OVER (PARTITION BY DATE_TRUNC('WEEK', paid_at) ORDER BY amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_amount
    FROM payments
    WINDOW w_part AS (PARTITION BY status ORDER BY amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
enriched AS (
    SELECT
        id,
        paid_at,
        status,
        amount_rank,
        amount_share,
        paid_at_to_date_avg_amount,
        LEAD(amount) OVER w_running next_amount,
        LAG(amount) OVER w_running prev_amount,
        LEAD(amount_share) OVER w_running next_amount_share,
        ROW_NUMBER() OVER w_running seq_no
    FROM with_windows
    WINDOW w_running AS (PARTITION BY status ORDER BY amount_share, id)
),
with_windows_payments AS (
    SELECT
        id,
        paid_at,
        amount_share,
        paid_at_to_date_avg_amount,
        next_amount,
        prev_amount,
        seq_no,
        LEAD(seq_no) OVER (PARTITION BY status ORDER BY seq_no, id) next_seq_no,
        LAG(seq_no) OVER (PARTITION BY status ORDER BY seq_no, id) prev_seq_no,
        LEAD(prev_amount) OVER (PARTITION BY status ORDER BY seq_no, id) next_prev_amount,
        ROW_NUMBER() OVER (PARTITION BY status ORDER BY seq_no, id) event_seq,
        ROW_NUMBER() OVER (ORDER BY next_amount DESC, id) event_seq_2
    FROM enriched
    WHERE seq_no <= 6
),
enriched_payments AS (
    SELECT
        *,
        LAST_VALUE(prev_amount, TRUE) OVER (ORDER BY event_seq_2, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) last_known_prev_amount,
        ROW_NUMBER() OVER (ORDER BY seq_no, id) event_seq_3,
        AVG(prev_seq_no) OVER (PARTITION BY TO_DATE(paid_at) ORDER BY next_amount, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) prev_seq_no_avg_6,
        NTILE(5) OVER (ORDER BY next_prev_amount, id) next_prev_amount_bucket
    FROM with_windows_payments
),
valid AS (
    SELECT
        id,
        paid_at,
        amount_share,
        paid_at_to_date_avg_amount,
        next_amount,
        prev_amount,
        next_seq_no,
        prev_seq_no,
        next_prev_amount,
        event_seq,
        event_seq_2,
        last_known_prev_amount,
        event_seq_3,
        prev_seq_no_avg_6,
        DATEDIFF(CURRENT_DATE(), paid_at) days_since_paid_at
    FROM enriched_payments
    WHERE event_seq != 4
),
metrics AS (
    SELECT
        *,
        SUM(event_seq_2) OVER (ORDER BY event_seq, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) cumulative_event_seq_2,
        NTILE(3) OVER w_part paid_at_to_date_avg_amount_bucket,
        NTILE(3) OVER (ORDER BY event_seq_3, id) event_seq_3_bucket
    FROM valid
    WINDOW w_part AS (ORDER BY paid_at_to_date_avg_amount, id)
),
metrics_payments AS (
    SELECT
        id,
        paid_at,
        amount_share,
        next_amount,
        prev_seq_no,
        event_seq,
        prev_seq_no_avg_6,
        days_since_paid_at,
        cumulative_event_seq_2,
        paid_at_to_date_avg_amount_bucket,
        event_seq_3_bucket,
        LAG(paid_at_to_date_avg_amount) OVER w_recent prev_paid_at_to_date_avg_amount,
        LEAD(next_prev_amount) OVER w_recent next_next_prev_amount,
        ROW_NUMBER() OVER w_recent seq_no,
        SUM(event_seq_3_bucket) OVER (PARTITION BY MONTH(paid_at) ORDER BY paid_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_event_seq_3_bucket
    FROM metrics
    WINDOW w_recent AS (ORDER BY paid_at_to_date_avg_amount, id)
)
SELECT
    CASE WHEN event_seq_3_bucket >= 7 THEN 'HIGH' ELSE 'LOW' END AS event_seq_3_bucket_tier,
    CASE WHEN days_since_paid_at >= 77 THEN 'PREMIUM' ELSE 'STANDARD' END AS days_since_paid_at_tier,
    MIN(paid_at) AS first_paid_at,
    (UNIX_TIMESTAMP(MAX(paid_at)) - UNIX_TIMESTAMP(MIN(paid_at))) / 60 AS span_minutes,
    APPROX_PERCENTILE(days_since_paid_at, 0.95) AS days_since_paid_at_p95,
    AVG(event_seq) AS avg_event_seq
FROM metrics_payments
GROUP BY GROUPING SETS ((CASE WHEN event_seq_3_bucket >= 7 THEN 'HIGH' ELSE 'LOW' END, CASE WHEN days_since_paid_at >= 77 THEN 'PREMIUM' ELSE 'STANDARD' END), (CASE WHEN event_seq_3_bucket >= 7 THEN 'HIGH' ELSE 'LOW' END))
ORDER BY span_minutes NULLS FIRST
