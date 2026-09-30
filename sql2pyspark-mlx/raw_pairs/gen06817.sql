WITH enriched AS (
    -- Attach orders attributes
    SELECT
        e.id,
        e.order_id,
        e.amount,
        p.amount AS order_amount,
        p.order_date
    FROM payments e
    LEFT JOIN orders p
        ON e.order_id <=> p.id
    WHERE p.shipping_fee <= 32
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        AVG(amount) OVER (ORDER BY order_date, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS amount_avg_6_prior,
        PERCENT_RANK() OVER (PARTITION BY order_id ORDER BY order_amount DESC) AS order_amount_percent_rank,
        LAG(order_amount, 1) OVER (PARTITION BY MONTH(order_date), order_id ORDER BY amount, id) AS prev_order_amount,
        ROW_NUMBER() OVER (ORDER BY order_date DESC, id) AS row_seq,
        SUM(amount) OVER (PARTITION BY DATE_TRUNC('WEEK', order_date), order_id) AS order_date_period_sum_amount
    FROM enriched
),
enriched_payments AS (
    -- Ranking and gap features
    SELECT
        id,
        order_id,
        SUM(row_seq) OVER (PARTITION BY order_id ORDER BY prev_order_amount, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS last4_sum_row_seq,
        MIN(row_seq) OVER (PARTITION BY order_id ORDER BY prev_order_amount, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS last4_min_row_seq,
        MAX(row_seq) OVER (PARTITION BY order_id ORDER BY prev_order_amount, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS last4_max_row_seq
    FROM with_windows
),
enriched_payments_2 AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        LAG(last4_max_row_seq) OVER (PARTITION BY order_id ORDER BY last4_sum_row_seq, id) AS prev_last4_max_row_seq,
        LEAD(last4_max_row_seq) OVER (PARTITION BY order_id ORDER BY last4_sum_row_seq, id) AS next_last4_max_row_seq,
        LEAD(last4_min_row_seq) OVER (PARTITION BY order_id ORDER BY last4_sum_row_seq, id) AS next_last4_min_row_seq,
        ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY last4_sum_row_seq, id) AS event_seq
    FROM enriched_payments
),
with_windows_payments AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        last4_min_row_seq,
        last4_max_row_seq,
        next_last4_max_row_seq,
        event_seq,
        LAG(last4_min_row_seq, 1) OVER w_grp AS prev_last4_min_row_seq,
        FIRST_VALUE(last4_max_row_seq, TRUE) OVER (PARTITION BY order_id ORDER BY event_seq, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_last4_max_row_seq,
        PERCENT_RANK() OVER w_hist AS next_last4_min_row_seq_percent_rank,
        PERCENT_RANK() OVER (PARTITION BY order_id ORDER BY next_last4_max_row_seq NULLS FIRST) AS next_last4_max_row_seq_percent_rank,
        FIRST_VALUE(last4_max_row_seq) IGNORE NULLS OVER w_part AS first_known_last4_max_row_seq_2
    FROM enriched_payments_2
    WINDOW w_grp AS (PARTITION BY order_id ORDER BY last4_max_row_seq, id), w_hist AS (PARTITION BY order_id ORDER BY next_last4_min_row_seq NULLS FIRST), w_part AS (PARTITION BY order_id ORDER BY last4_min_row_seq, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        COUNT(event_seq) OVER (ORDER BY last4_min_row_seq, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS last3_count_event_seq,
        MIN(event_seq) OVER (ORDER BY last4_min_row_seq, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS last3_min_event_seq,
        AVG(event_seq) OVER (ORDER BY last4_min_row_seq, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS last3_avg_event_seq
    FROM with_windows_payments
)
-- Grouped metrics
SELECT
    CASE WHEN last3_avg_event_seq >= 7.6 THEN 'PREMIUM' ELSE 'STANDARD' END AS last3_avg_event_seq_tier,
    CASE WHEN prev_last4_min_row_seq >= 6.8 THEN 'HIGH' ELSE 'LOW' END AS prev_last4_min_row_seq_tier,
    CASE WHEN next_last4_min_row_seq_percent_rank >= 0.8 THEN 'LARGE' ELSE 'SMALL' END AS next_last4_min_row_seq_percent_rank_tier,
    AVG(event_seq) AS avg_event_seq,
    PERCENTILE_APPROX(first_known_last4_max_row_seq_2, ARRAY(0.25, 0.5, 0.75)) AS first_known_last4_max_row_seq_2_quartiles,
    COUNT(*) AS cnt
FROM scored
GROUP BY last3_avg_event_seq_tier, prev_last4_min_row_seq_tier, next_last4_min_row_seq_percent_rank_tier;
