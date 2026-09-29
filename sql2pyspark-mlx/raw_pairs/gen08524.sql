WITH valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, DATE_SUB(start_date, 33) AS start_date_minus_33d
    FROM subscriptions
    WHERE plan NOT IN ('basic', 'standard')
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        plan,
        end_date,
        start_date_minus_33d,
        LAG(monthly_fee) OVER w_hist AS prev_monthly_fee,
        LEAD(monthly_fee) OVER w_hist AS next_monthly_fee,
        ROW_NUMBER() OVER w_hist AS event_seq,
        LAST_VALUE(monthly_fee) IGNORE NULLS OVER w_running AS last_known_monthly_fee
    FROM valid
    WHERE monthly_fee IS NOT NULL
    WINDOW w_hist AS (PARTITION BY plan, status ORDER BY end_date ASC, id ASC), w_running AS (PARTITION BY customer_id ORDER BY start_date ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
kept_keys AS (
    -- Set intersection on keys
    SELECT id
    FROM metrics
    INTERSECT ALL
    SELECT id
    FROM subscriptions
    WHERE plan = 'enterprise'
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM metrics s
    INNER JOIN kept_keys k ON s.id = k.id
),
metrics_subscriptions AS (
    -- Running and moving aggregates
    SELECT
        next_monthly_fee,
        event_seq,
        last_known_monthly_fee,
        SUM(prev_monthly_fee) OVER w_hist AS end_date_month_sum_prev_monthly_fee,
        DENSE_RANK() OVER (ORDER BY prev_monthly_fee DESC) AS prev_monthly_fee_dense_rank,
        SUM(last_known_monthly_fee) OVER w_hist AS end_date_month_sum_last_known_monthly_fee,
        ROUND(prev_monthly_fee / SUM(prev_monthly_fee) OVER (PARTITION BY YEAR(end_date), plan), 4) AS prev_monthly_fee_share
    FROM verified
    WINDOW w_hist AS (PARTITION BY MONTH(end_date), plan)
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN prev_monthly_fee_dense_rank >= 9 THEN 'PREMIUM' ELSE 'STANDARD' END AS prev_monthly_fee_dense_rank_tier,
    CASE WHEN end_date_month_sum_last_known_monthly_fee >= 1192 THEN 'HIGH' ELSE 'LOW' END AS end_date_month_sum_last_known_monthly_fee_tier,
    PERCENTILE_APPROX(last_known_monthly_fee, ARRAY(0.25, 0.5, 0.75)) AS last_known_monthly_fee_quartiles,
    MIN(event_seq) AS min_event_seq,
    MIN(end_date_month_sum_prev_monthly_fee) AS min_end_date_month_sum_prev_monthly_fee,
    COUNT(*) AS row_count
FROM metrics_subscriptions
GROUP BY 1, 2
ORDER BY min_event_seq DESC NULLS LAST;
