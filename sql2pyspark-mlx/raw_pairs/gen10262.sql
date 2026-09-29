WITH metrics AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        order_id,
        method,
        amount,
        paid_at,
        status,
        PERCENT_RANK() OVER (PARTITION BY method ORDER BY amount DESC NULLS FIRST) AS amount_percent_rank,
        ROW_NUMBER() OVER (PARTITION BY method ORDER BY paid_at DESC, id) AS event_seq
    FROM payments
    WHERE method IN ('promptpay', 'cash')
)
-- Aggregate with conditional metrics
SELECT
    order_id,
    method,
    (UNIX_TIMESTAMP(MAX(paid_at)) - UNIX_TIMESTAMP(MIN(paid_at))) / 60 AS duration_min,
    SUM(event_seq) AS total_event_seq,
    MAX(paid_at) AS last_paid_at,
    SUM(CASE WHEN method = 'promptpay' THEN amount_percent_rank ELSE 0 END) AS promptpay_amount_percent_rank,
    PERCENTILE_APPROX(event_seq, ARRAY(0.25, 0.5, 0.75)) AS event_seq_quartiles
FROM metrics
GROUP BY order_id, method
ORDER BY total_event_seq NULLS FIRST;
