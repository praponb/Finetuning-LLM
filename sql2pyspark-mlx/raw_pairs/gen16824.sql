WITH with_windows AS (
    SELECT
        *,
        LAG(monthly_fee) OVER (PARTITION BY plan, customer_id ORDER BY start_date, id) AS prev_monthly_fee,
        ROW_NUMBER() OVER (PARTITION BY plan, customer_id ORDER BY start_date, id) AS row_seq
    FROM subscriptions
),
scored AS (
    SELECT
        *,
        NTILE(5) OVER (PARTITION BY MONTH(start_date), customer_id ORDER BY prev_monthly_fee DESC, id) AS prev_monthly_fee_bucket,
        LAST_VALUE(monthly_fee, TRUE) OVER (PARTITION BY customer_id ORDER BY prev_monthly_fee, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_monthly_fee,
        DENSE_RANK() OVER w_grp AS prev_monthly_fee_dense_rank,
        NTILE(10) OVER (PARTITION BY status ORDER BY prev_monthly_fee DESC, id) AS prev_monthly_fee_bucket_2
    FROM with_windows
    WINDOW w_grp AS (PARTITION BY customer_id ORDER BY prev_monthly_fee DESC)
),
metrics AS (
    SELECT
        *,
        SUM(row_seq) OVER (ORDER BY end_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_row_seq,
        SUM(prev_monthly_fee_bucket_2) OVER (ORDER BY prev_monthly_fee_bucket, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_prev_monthly_fee_bucket_2,
        ROUND(monthly_fee / SUM(monthly_fee) OVER (PARTITION BY status, plan), 4) AS monthly_fee_share
    FROM scored
    WHERE row_seq <= 4
),
ranked AS (
    SELECT
        id,
        plan,
        end_date,
        monthly_fee,
        status,
        prev_monthly_fee,
        last_known_monthly_fee,
        prev_monthly_fee_dense_rank,
        running_row_seq,
        cumulative_prev_monthly_fee_bucket_2,
        monthly_fee_share
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY plan, customer_id ORDER BY prev_monthly_fee_dense_rank NULLS LAST, id) AS latest_rank
        FROM metrics
    ) x
    WHERE latest_rank = 1
)
SELECT
    id,
    end_date,
    monthly_fee,
    prev_monthly_fee,
    last_known_monthly_fee,
    running_row_seq,
    ROW_NUMBER() OVER (PARTITION BY plan ORDER BY cumulative_prev_monthly_fee_bucket_2 DESC, id) AS seq_no,
    NTILE(5) OVER w_running AS cumulative_prev_monthly_fee_bucket_2_bucket,
    NTILE(10) OVER w_running AS cumulative_prev_monthly_fee_bucket_2_bucket_2,
    LEAD(cumulative_prev_monthly_fee_bucket_2, 2) OVER (PARTITION BY plan, status ORDER BY prev_monthly_fee, id) AS next_cumulative_prev_monthly_fee_bucket_2
FROM ranked
WINDOW w_running AS (PARTITION BY status ORDER BY cumulative_prev_monthly_fee_bucket_2 DESC, id)
