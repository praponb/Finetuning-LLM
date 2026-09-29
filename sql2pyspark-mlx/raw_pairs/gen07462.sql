WITH filtered AS (
    SELECT *, UPPER(status) AS status_upper
    FROM payments
    WHERE method IS NOT NULL
),
with_windows AS (
    SELECT
        id,
        amount,
        paid_at,
        LEAD(amount, 2, 0) OVER (PARTITION BY TO_DATE(paid_at), order_id ORDER BY paid_at, id) AS next_amount,
        AVG(amount) OVER (PARTITION BY method, status ORDER BY amount, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS amount_avg_3_prior,
        ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY paid_at, id) AS seq_no
    FROM filtered
    WHERE order_id IN (7, 9, 13, 30)
),
with_windows_payments AS (
    SELECT
        *,
        SUM(amount_avg_3_prior) OVER (ORDER BY CAST(paid_at AS BIGINT) RANGE BETWEEN 604800 PRECEDING AND CURRENT ROW) AS amount_avg_3_prior_sum_7d,
        SUM(amount) OVER (ORDER BY next_amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_amount,
        MAX(amount_avg_3_prior) OVER w_part AS amount_avg_3_prior_max_7d
    FROM with_windows
    WINDOW w_part AS (PARTITION BY YEAR(paid_at) ORDER BY CAST(paid_at AS BIGINT) RANGE BETWEEN 604800 PRECEDING AND CURRENT ROW)
),
enriched AS (
    SELECT
        *,
        NTILE(10) OVER (ORDER BY amount_avg_3_prior DESC, id) AS amount_avg_3_prior_bucket,
        LAG(cumulative_amount, 2) OVER (PARTITION BY DATE_TRUNC('MONTH', paid_at) ORDER BY amount_avg_3_prior_sum_7d, id) AS prev_cumulative_amount,
        ROW_NUMBER() OVER (ORDER BY cumulative_amount, id) AS row_seq
    FROM with_windows_payments
),
enriched_payments AS (
    SELECT
        id,
        paid_at,
        amount_avg_3_prior,
        cumulative_amount,
        amount_avg_3_prior_max_7d,
        prev_cumulative_amount,
        SUM(seq_no) OVER w_running AS running_sum_seq_no,
        MAX(seq_no) OVER w_running AS running_max_seq_no,
        MIN(seq_no) OVER w_running AS running_min_seq_no,
        COUNT(seq_no) OVER w_running AS running_count_seq_no
    FROM enriched
    WINDOW w_running AS (ORDER BY amount_avg_3_prior, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
)
SELECT
    id,
    amount_avg_3_prior,
    amount_avg_3_prior_max_7d,
    prev_cumulative_amount,
    running_sum_seq_no,
    running_min_seq_no,
    running_count_seq_no
FROM enriched_payments
ORDER BY amount_avg_3_prior_max_7d DESC NULLS LAST, running_min_seq_no DESC
