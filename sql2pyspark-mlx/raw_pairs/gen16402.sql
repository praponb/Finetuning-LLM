WITH base AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        customer_id,
        plan,
        start_date,
        monthly_fee,
        CEIL(monthly_fee) AS monthly_fee_whole
    FROM subscriptions
    WHERE customer_id IS NOT NULL
),
ranked AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY customer_id, plan ORDER BY start_date DESC NULLS LAST, id) AS latest_rank
        FROM base
    ) ranked
    WHERE latest_rank = 1
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        plan,
        monthly_fee_whole,
        latest_rank,
        FIRST_VALUE(monthly_fee_whole) IGNORE NULLS OVER (PARTITION BY DATE_TRUNC('YEAR', start_date), customer_id ORDER BY start_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_monthly_fee_whole,
        ROW_NUMBER() OVER (PARTITION BY plan ORDER BY start_date, id) AS row_seq,
        CUME_DIST() OVER (PARTITION BY plan, customer_id ORDER BY monthly_fee_whole DESC NULLS LAST) AS monthly_fee_whole_cume_dist
    FROM ranked
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        latest_rank,
        ROUND(row_seq / SUM(row_seq) OVER (PARTITION BY plan), 4) AS row_seq_share,
        ROW_NUMBER() OVER (PARTITION BY plan ORDER BY latest_rank, id) AS row_seq_2
    FROM enriched
    WHERE latest_rank <> 1
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        row_seq_2,
        SUM(row_seq_2) OVER w_hist AS running_sum_row_seq_2,
        COUNT(row_seq_2) OVER w_hist AS running_count_row_seq_2,
        MAX(row_seq_2) OVER w_hist AS running_max_row_seq_2,
        AVG(row_seq_2) OVER w_hist AS running_avg_row_seq_2,
        ROW_NUMBER() OVER w_grp AS row_seq
    FROM metrics
    WINDOW w_hist AS (ORDER BY row_seq_2, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), w_grp AS (ORDER BY row_seq_2, id)
)
-- Running and moving aggregates
SELECT
    id,
    row_seq_2,
    running_sum_row_seq_2,
    running_count_row_seq_2,
    row_seq,
    SUM(running_max_row_seq_2) OVER w_recent AS last3_sum_running_max_row_seq_2,
    MAX(running_max_row_seq_2) OVER w_recent AS last3_max_running_max_row_seq_2,
    COUNT(running_max_row_seq_2) OVER w_recent AS last3_count_running_max_row_seq_2,
    AVG(running_max_row_seq_2) OVER w_recent AS last3_avg_running_max_row_seq_2,
    FIRST_VALUE(row_seq_2, TRUE) OVER w_part AS first_known_row_seq_2
FROM with_windows
WINDOW w_recent AS (ORDER BY running_max_row_seq_2, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), w_part AS (ORDER BY running_count_row_seq_2, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
