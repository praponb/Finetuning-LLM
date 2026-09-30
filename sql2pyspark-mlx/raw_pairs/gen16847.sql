WITH with_dims AS (
    -- Attach orders attributes
    SELECT
        src.id,
        src.order_id,
        src.amount,
        src.paid_at,
        src.status,
        p.status AS order_status,
        p.order_date
    FROM payments src
    INNER JOIN orders p
        ON src.order_id = p.id
    WHERE p.shipping_fee <= 26
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        amount,
        status,
        order_status,
        order_date,
        SUM(amount) OVER (PARTITION BY order_status ORDER BY paid_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_amount,
        LAG(amount, 1, 0) OVER (ORDER BY paid_at, id) AS prev_amount,
        LAG(amount, 2) OVER (PARTITION BY status ORDER BY paid_at, id) AS prev_amount_2,
        AVG(amount) OVER (PARTITION BY DATE_TRUNC('YEAR', order_date), order_id ORDER BY order_date, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS amount_avg_3_prior,
        ROW_NUMBER() OVER (PARTITION BY status ORDER BY order_date DESC, id) AS seq_no
    FROM with_dims
),
with_windows_payments AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        AVG(prev_amount) OVER (PARTITION BY order_status, status ORDER BY order_date, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS prev_amount_avg_6,
        ROUND(amount_avg_3_prior / SUM(amount_avg_3_prior) OVER (PARTITION BY order_status), 4) AS amount_avg_3_prior_share,
        CUME_DIST() OVER (PARTITION BY YEAR(order_date), order_status ORDER BY prev_amount_2) AS prev_amount_2_cume_dist
    FROM with_windows
),
ranked AS (
    -- Keep one row per key (latest first)
    SELECT
        id,
        status,
        order_status,
        prev_amount,
        prev_amount_2,
        amount_avg_3_prior,
        seq_no,
        amount_avg_3_prior_share
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY order_status ORDER BY amount DESC, id) AS dedup_rank
        FROM with_windows_payments
    ) x
    WHERE dedup_rank = 1
),
latest AS (
    -- QUALIFY-style window filter
    SELECT *, ROW_NUMBER() OVER (PARTITION BY status, order_status ORDER BY prev_amount_2 DESC NULLS FIRST, id) AS dedup_rank
    FROM ranked
    QUALIFY dedup_rank = 1
)
-- Grouped metrics
SELECT
    seq_no,
    order_status,
    CASE WHEN seq_no >= 8 THEN 'LARGE' ELSE 'SMALL' END AS seq_no_tier,
    PERCENTILE_APPROX(amount_avg_3_prior_share, 0.5) AS amount_avg_3_prior_share_p50,
    ROUND(AVG(seq_no), 2) AS avg_seq_no,
    COUNT(CASE WHEN status = 'REFUNDED' THEN 1 END) AS refunded_count,
    COUNT_IF(status = 'FAILED') AS failed_events,
    COUNT(*) FILTER (WHERE order_status = 'COMPLETED') AS completed_rows
FROM latest
GROUP BY ROLLUP (seq_no, order_status, CASE WHEN seq_no >= 8 THEN 'LARGE' ELSE 'SMALL' END)
HAVING completed_rows < 1000;
