WITH valid AS (
    SELECT
        id,
        order_id,
        method,
        amount,
        status,
        ROUND(amount, 0) AS amount_rounded
    FROM payments
    WHERE paid_at >= DATE '2023-08-05'
),
latest AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY method, order_id ORDER BY amount_rounded DESC, id) AS latest_rank
    FROM valid
    QUALIFY latest_rank = 1
),
metrics AS (
    SELECT
        *,
        SUM(amount) OVER w_ordered AS running_sum_amount,
        AVG(amount) OVER w_ordered AS running_avg_amount,
        MIN(amount) OVER w_ordered AS running_min_amount,
        MAX(amount) OVER w_ordered AS running_max_amount
    FROM latest
    WINDOW w_ordered AS (PARTITION BY method ORDER BY amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
scored AS (
    SELECT
        id,
        order_id,
        method,
        latest_rank,
        running_sum_amount,
        running_avg_amount,
        running_min_amount,
        running_max_amount,
        MIN(running_max_amount) OVER (PARTITION BY method ORDER BY running_avg_amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_min_running_max_amount,
        MAX(running_max_amount) OVER (PARTITION BY method ORDER BY running_avg_amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_max_running_max_amount,
        COUNT(running_max_amount) OVER (PARTITION BY method ORDER BY running_avg_amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count_running_max_amount,
        LEAD(amount, 2) OVER (PARTITION BY order_id ORDER BY running_min_amount, id) AS next_amount
    FROM metrics
    WHERE amount_rounded < 38
),
metrics_payments AS (
    SELECT
        id,
        running_sum_amount,
        running_avg_amount,
        running_min_running_max_amount,
        running_count_running_max_amount,
        COUNT(running_min_running_max_amount) OVER (PARTITION BY order_id, method ORDER BY running_max_amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count_running_min_running_max_amount,
        AVG(running_min_running_max_amount) OVER (PARTITION BY order_id, method ORDER BY running_max_amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_avg_running_min_running_max_amount,
        MAX(running_min_running_max_amount) OVER (PARTITION BY order_id, method ORDER BY running_max_amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_max_running_min_running_max_amount,
        RANK() OVER (PARTITION BY method, order_id ORDER BY next_amount NULLS LAST) AS next_amount_rank
    FROM scored
),
with_windows AS (
    SELECT
        id,
        running_avg_amount,
        running_count_running_max_amount,
        running_count_running_min_running_max_amount,
        running_avg_running_min_running_max_amount,
        next_amount_rank,
        LEAD(running_count_running_max_amount, 1) OVER (ORDER BY running_max_running_min_running_max_amount, id) AS next_running_count_running_max_amount,
        ROW_NUMBER() OVER (ORDER BY next_amount_rank DESC, id) AS seq_no,
        FIRST_VALUE(running_avg_amount) IGNORE NULLS OVER (ORDER BY running_sum_amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_running_avg_amount,
        SUM(running_avg_running_min_running_max_amount) OVER (ORDER BY running_count_running_min_running_max_amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_running_avg_running_min_running_max_amount,
        NTILE(5) OVER (ORDER BY next_amount_rank DESC, id) AS next_amount_rank_bucket
    FROM metrics_payments
)
SELECT
    seq_no,
    MAX(running_count_running_max_amount) AS max_running_count_running_max_amount,
    ROUND(AVG(next_amount_rank_bucket), 2) AS avg_next_amount_rank_bucket,
    PERCENTILE_APPROX(cumulative_running_avg_running_min_running_max_amount, ARRAY(0.25, 0.5, 0.75)) AS cumulative_running_avg_running_min_running_max_amount_quartiles
FROM with_windows
GROUP BY seq_no
HAVING MAX(running_count_running_max_amount) > 0
ORDER BY max_running_count_running_max_amount DESC;
