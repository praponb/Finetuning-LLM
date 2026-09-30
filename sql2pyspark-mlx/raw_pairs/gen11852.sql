WITH filtered AS (
    SELECT
        id,
        customer_id,
        start_date,
        end_date,
        status,
        DATEDIFF(end_date, start_date) days_start_date_to_end_date
    FROM subscriptions
    WHERE monthly_fee > 160
),
scored AS (
    SELECT
        id,
        customer_id,
        status,
        days_start_date_to_end_date,
        LAG(days_start_date_to_end_date) OVER w_ordered prev_days_start_date_to_end_date,
        LEAD(days_start_date_to_end_date) OVER w_ordered next_days_start_date_to_end_date,
        ROW_NUMBER() OVER w_ordered row_seq,
        LEAD(days_start_date_to_end_date, 2, 0) OVER (PARTITION BY YEAR(end_date), customer_id ORDER BY end_date ASC, id ASC) next_days_start_date_to_end_date_2
    FROM filtered
    WINDOW w_ordered AS (PARTITION BY status ORDER BY start_date ASC, id ASC)
),
with_windows AS (
    SELECT
        *,
        LAG(next_days_start_date_to_end_date, 1) OVER (PARTITION BY customer_id ORDER BY next_days_start_date_to_end_date_2 ASC, id ASC) prev_next_days_start_date_to_end_date,
        AVG(next_days_start_date_to_end_date) OVER (PARTITION BY status ORDER BY days_start_date_to_end_date ASC, id ASC ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) next_days_start_date_to_end_date_avg_3,
        RANK() OVER (ORDER BY prev_days_start_date_to_end_date DESC NULLS LAST) prev_days_start_date_to_end_date_rank
    FROM scored
),
metrics AS (
    SELECT
        id,
        prev_days_start_date_to_end_date,
        next_days_start_date_to_end_date,
        row_seq,
        next_days_start_date_to_end_date_2,
        prev_next_days_start_date_to_end_date,
        next_days_start_date_to_end_date_avg_3,
        prev_days_start_date_to_end_date_rank,
        MAX(days_start_date_to_end_date) OVER w_part last7_max_days_start_date_to_end_date,
        COUNT(days_start_date_to_end_date) OVER w_part last7_count_days_start_date_to_end_date,
        SUM(days_start_date_to_end_date) OVER w_part last7_sum_days_start_date_to_end_date,
        AVG(days_start_date_to_end_date) OVER w_part last7_avg_days_start_date_to_end_date,
        FIRST_VALUE(prev_days_start_date_to_end_date_rank, TRUE) OVER w_grp first_known_prev_days_start_date_to_end_date_rank
    FROM with_windows
    WINDOW w_part AS (PARTITION BY status ORDER BY prev_days_start_date_to_end_date ASC, id ASC ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), w_grp AS (PARTITION BY status, customer_id ORDER BY prev_next_days_start_date_to_end_date ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
metrics_subscriptions AS (
    SELECT
        id,
        prev_days_start_date_to_end_date,
        row_seq,
        prev_days_start_date_to_end_date_rank,
        last7_count_days_start_date_to_end_date,
        LEAD(next_days_start_date_to_end_date_avg_3) OVER w_recent next_next_days_start_date_to_end_date_avg_3,
        LAG(next_days_start_date_to_end_date_avg_3) OVER w_recent prev_next_days_start_date_to_end_date_avg_3,
        LAG(next_days_start_date_to_end_date) OVER w_recent prev_next_days_start_date_to_end_date_2,
        LEAD(next_days_start_date_to_end_date) OVER w_recent next_next_days_start_date_to_end_date,
        ROW_NUMBER() OVER w_recent seq_no
    FROM metrics
    WHERE prev_days_start_date_to_end_date_rank > 4
    WINDOW w_recent AS (ORDER BY last7_sum_days_start_date_to_end_date ASC, id ASC)
),
valid AS (
    SELECT *, CAST(prev_days_start_date_to_end_date_rank AS STRING) prev_days_start_date_to_end_date_rank_str
    FROM metrics_subscriptions
    WHERE next_next_days_start_date_to_end_date_avg_3 BETWEEN 24 AND 59
)
SELECT
    id,
    row_seq,
    prev_days_start_date_to_end_date_rank,
    last7_count_days_start_date_to_end_date,
    prev_next_days_start_date_to_end_date_2,
    next_next_days_start_date_to_end_date,
    seq_no,
    prev_days_start_date_to_end_date_rank_str
FROM valid
ORDER BY seq_no ASC
