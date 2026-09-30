WITH cleaned AS (
    -- Basic cleansing and defaults
    SELECT *, FLOOR(monthly_fee) AS monthly_fee_whole
    FROM subscriptions
    WHERE start_date > DATE '2023-06-29'
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        end_date,
        monthly_fee,
        PERCENT_RANK() OVER w_part AS monthly_fee_percent_rank,
        AVG(monthly_fee) OVER w_recent AS monthly_fee_avg_3_prior,
        CUME_DIST() OVER (ORDER BY monthly_fee DESC NULLS FIRST) AS monthly_fee_cume_dist,
        NTILE(10) OVER (ORDER BY monthly_fee DESC, id) AS monthly_fee_bucket,
        FIRST_VALUE(monthly_fee_whole) IGNORE NULLS OVER (PARTITION BY status ORDER BY start_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_monthly_fee_whole
    FROM cleaned
    WINDOW w_part AS (PARTITION BY plan, customer_id ORDER BY monthly_fee DESC NULLS LAST), w_recent AS (ORDER BY end_date, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING)
),
enriched AS (
    -- Ranking and gap features
    SELECT
        *,
        SUM(first_known_monthly_fee_whole) OVER (ORDER BY first_known_monthly_fee_whole, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_first_known_monthly_fee_whole,
        SUM(monthly_fee_cume_dist) OVER (ORDER BY end_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_monthly_fee_cume_dist
    FROM metrics
),
metrics_subscriptions AS (
    -- Ranking and gap features
    SELECT
        id,
        end_date,
        monthly_fee,
        monthly_fee_percent_rank,
        monthly_fee_bucket,
        running_first_known_monthly_fee_whole,
        SUM(monthly_fee_avg_3_prior) OVER w_part AS cumulative_monthly_fee_avg_3_prior,
        NTILE(3) OVER (ORDER BY monthly_fee_cume_dist DESC, id) AS monthly_fee_cume_dist_bucket,
        LEAD(monthly_fee_bucket, 1) OVER (ORDER BY first_known_monthly_fee_whole, id) AS next_monthly_fee_bucket,
        ROW_NUMBER() OVER w_grp AS row_seq
    FROM enriched
    WINDOW w_part AS (PARTITION BY DATE_TRUNC('YEAR', end_date) ORDER BY running_first_known_monthly_fee_whole, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), w_grp AS (ORDER BY end_date, id)
),
scored AS (
    -- Ranking and gap features
    SELECT
        *,
        NTILE(10) OVER (ORDER BY running_first_known_monthly_fee_whole, id) AS running_first_known_monthly_fee_whole_bucket,
        LAG(cumulative_monthly_fee_avg_3_prior, 1) OVER (ORDER BY monthly_fee_bucket, id) AS prev_cumulative_monthly_fee_avg_3_prior,
        LAG(monthly_fee_cume_dist_bucket, 2, 0) OVER (PARTITION BY MONTH(end_date) ORDER BY monthly_fee_bucket, id) AS prev_monthly_fee_cume_dist_bucket,
        ROW_NUMBER() OVER (ORDER BY monthly_fee_percent_rank, id) AS row_seq_2
    FROM metrics_subscriptions
),
filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        LEAST(row_seq, monthly_fee_cume_dist_bucket) AS least_row_seq_monthly_fee_cume_dist_bucket,
        CASE WHEN monthly_fee >= 183 THEN 'High' WHEN monthly_fee >= 88 THEN 'Medium' ELSE 'Low' END AS monthly_fee_level
    FROM scored
    WHERE prev_cumulative_monthly_fee_avg_3_prior <= 371
),
metrics_subscriptions_2 AS (
    -- Running and moving aggregates
    SELECT
        id,
        end_date,
        monthly_fee,
        monthly_fee_percent_rank,
        running_first_known_monthly_fee_whole,
        cumulative_monthly_fee_avg_3_prior,
        monthly_fee_cume_dist_bucket,
        next_monthly_fee_bucket,
        LAST_VALUE(cumulative_monthly_fee_avg_3_prior, TRUE) OVER (ORDER BY running_first_known_monthly_fee_whole_bucket, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_cumulative_monthly_fee_avg_3_prior,
        AVG(cumulative_monthly_fee_avg_3_prior) OVER (ORDER BY least_row_seq_monthly_fee_cume_dist_bucket, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS cumulative_monthly_fee_avg_3_prior_avg_3_prior,
        SUM(running_first_known_monthly_fee_whole_bucket) OVER (ORDER BY row_seq, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_running_first_known_monthly_fee_whole_bucket,
        DENSE_RANK() OVER (ORDER BY monthly_fee DESC) AS monthly_fee_dense_rank
    FROM filtered
)
SELECT
    id,
    end_date,
    monthly_fee_percent_rank,
    running_first_known_monthly_fee_whole,
    cumulative_monthly_fee_avg_3_prior,
    monthly_fee_cume_dist_bucket,
    next_monthly_fee_bucket,
    cumulative_running_first_known_monthly_fee_whole_bucket,
    monthly_fee_dense_rank
FROM metrics_subscriptions_2
ORDER BY id DESC
