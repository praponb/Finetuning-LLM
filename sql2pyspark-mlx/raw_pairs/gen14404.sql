WITH enriched AS (
    SELECT
        e.id,
        e.order_id,
        e.method,
        e.amount,
        e.paid_at,
        d.status AS order_status
    FROM payments e
    LEFT JOIN orders d
        ON e.order_id <=> d.id
    WHERE d.amount > 820
),
scored AS (
    SELECT
        id,
        method,
        SUM(amount) OVER (ORDER BY paid_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_amount,
        ROUND(amount / SUM(amount) OVER (PARTITION BY order_status), 4) AS amount_share,
        SUM(amount) OVER (PARTITION BY MONTH(paid_at) ORDER BY amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_amount,
        LAG(amount, 2) OVER (PARTITION BY YEAR(paid_at), method ORDER BY paid_at, id) AS prev_amount
    FROM enriched
),
filtered AS (
    SELECT *, ROUND(running_amount * 1.1, 2) AS running_amount_scaled
    FROM scored
    WHERE amount_share < 0.27
),
scored_payments AS (
    SELECT
        *,
        ROW_NUMBER() OVER w_hist AS event_seq,
        ROW_NUMBER() OVER w_recent AS event_seq_2,
        CUME_DIST() OVER w_recent_2 AS running_amount_cume_dist,
        NTILE(10) OVER (PARTITION BY method ORDER BY prev_amount, id) AS prev_amount_bucket
    FROM filtered
    WINDOW w_hist AS (PARTITION BY method ORDER BY running_amount DESC, id), w_recent AS (ORDER BY running_amount_scaled, id), w_recent_2 AS (PARTITION BY method ORDER BY running_amount DESC NULLS FIRST)
),
cleaned AS (
    SELECT
        id,
        method,
        cumulative_amount,
        amount_share,
        prev_amount,
        event_seq,
        prev_amount_bucket,
        UPPER(method) AS method_upper,
        GREATEST(running_amount_scaled, cumulative_amount) AS greatest_running_amount_scaled_cumulative_amount
    FROM scored_payments
    WHERE cumulative_amount = 7051
),
with_windows AS (
    SELECT
        *,
        LAST_VALUE(prev_amount) IGNORE NULLS OVER (PARTITION BY method ORDER BY prev_amount_bucket, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_prev_amount,
        FIRST_VALUE(cumulative_amount) IGNORE NULLS OVER (PARTITION BY method ORDER BY event_seq, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_cumulative_amount
    FROM cleaned
),
filtered_payments AS (
    SELECT
        *,
        ROUND(cumulative_amount / prev_amount, 2) AS cumulative_amount_per_prev_amount,
        cumulative_amount - prev_amount AS cumulative_amount_minus_prev_amount
    FROM with_windows
    WHERE method IS NOT NULL
      AND greatest_running_amount_scaled_cumulative_amount BETWEEN 31 AND 36
)
SELECT
    id,
    method,
    cumulative_amount,
    amount_share,
    prev_amount,
    prev_amount_bucket,
    method_upper,
    greatest_running_amount_scaled_cumulative_amount,
    first_known_cumulative_amount
FROM filtered_payments
ORDER BY amount_share DESC;
