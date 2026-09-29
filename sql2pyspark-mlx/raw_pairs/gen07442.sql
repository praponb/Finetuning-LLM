WITH base AS (
    -- Keep recent, valid rows
    SELECT
        id,
        order_id,
        method,
        amount,
        status,
        CASE WHEN method IN ('card', 'paypal') THEN 'Priority' ELSE 'Other' END AS method_group
    FROM payments
    WHERE paid_at > TIMESTAMP '2023-08-31 23:00:00'
      AND order_id IS NOT NULL
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        LAG(amount) OVER (PARTITION BY method ORDER BY amount, id) AS prev_amount,
        ROW_NUMBER() OVER (PARTITION BY method ORDER BY amount, id) AS row_seq
    FROM base
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        order_id,
        method_group,
        prev_amount,
        SUM(prev_amount) OVER (PARTITION BY method ORDER BY row_seq, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_sum_prev_amount,
        MIN(prev_amount) OVER (PARTITION BY method ORDER BY row_seq, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_min_prev_amount,
        COUNT(prev_amount) OVER (PARTITION BY method ORDER BY row_seq, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count_prev_amount,
        ROW_NUMBER() OVER (ORDER BY row_seq, id) AS seq_no
    FROM with_windows
),
latest AS (
    -- QUALIFY-style window filter
    SELECT
        id,
        order_id,
        method_group,
        prev_amount,
        running_sum_prev_amount,
        running_min_prev_amount,
        running_count_prev_amount,
        seq_no
    FROM scored
    QUALIFY ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY running_sum_prev_amount DESC NULLS LAST, id) = 1
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        running_sum_prev_amount,
        running_min_prev_amount,
        running_count_prev_amount,
        seq_no,
        GREATEST(running_min_prev_amount, prev_amount) AS greatest_running_min_prev_amount_prev_amount
    FROM latest
    WHERE prev_amount BETWEEN 630 AND 1160
),
with_windows_payments AS (
    -- Running and moving aggregates
    SELECT
        *,
        LEAD(greatest_running_min_prev_amount_prev_amount) OVER (ORDER BY seq_no, id) AS next_greatest_running_min_prev_amount_prev_amount,
        LAG(seq_no) OVER (ORDER BY seq_no, id) AS prev_seq_no,
        LEAD(seq_no) OVER (ORDER BY seq_no, id) AS next_seq_no,
        ROW_NUMBER() OVER (ORDER BY seq_no, id) AS row_seq
    FROM cleaned
)
-- Aggregate with conditional metrics
SELECT
    prev_seq_no,
    CASE WHEN running_sum_prev_amount >= 7100 THEN 'HIGH' ELSE 'LOW' END AS running_sum_prev_amount_tier,
    COUNT(*) AS num_rows,
    COUNT(DISTINCT id) AS distinct_ids,
    APPROX_PERCENTILE(running_sum_prev_amount, ARRAY(0.25, 0.5, 0.75)) AS running_sum_prev_amount_quartiles
FROM with_windows_payments
GROUP BY GROUPING SETS ((prev_seq_no, CASE WHEN running_sum_prev_amount >= 7100 THEN 'HIGH' ELSE 'LOW' END), (prev_seq_no))
HAVING distinct_ids >= 1;
