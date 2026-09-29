WITH valid AS (
    -- Basic cleansing and defaults
    SELECT *, DATEDIFF(end_date, start_date) AS days_start_date_to_end_date
    FROM subscriptions
    WHERE start_date > DATE '2023-10-05'
      AND customer_id IS NOT NULL
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        plan,
        start_date,
        end_date,
        monthly_fee,
        days_start_date_to_end_date,
        AVG(monthly_fee) OVER (PARTITION BY customer_id ORDER BY start_date, id ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS monthly_fee_avg_5,
        FIRST_VALUE(monthly_fee) IGNORE NULLS OVER (PARTITION BY customer_id ORDER BY start_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_monthly_fee
    FROM valid
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        MAX(days_start_date_to_end_date) OVER (PARTITION BY plan ORDER BY monthly_fee_avg_5, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_max_days_start_date_to_end_date,
        AVG(days_start_date_to_end_date) OVER (PARTITION BY plan ORDER BY monthly_fee_avg_5, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_avg_days_start_date_to_end_date,
        SUM(days_start_date_to_end_date) OVER (PARTITION BY plan ORDER BY monthly_fee_avg_5, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_sum_days_start_date_to_end_date
    FROM scored
),
ranked AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY plan ORDER BY first_known_monthly_fee DESC NULLS FIRST, id) AS rank_in_group
        FROM metrics
    ) x
    WHERE rank_in_group = 1
),
scored_subscriptions AS (
    -- Running and moving aggregates
    SELECT
        id,
        plan,
        start_date,
        end_date,
        days_start_date_to_end_date,
        monthly_fee_avg_5,
        running_avg_days_start_date_to_end_date,
        SUM(running_max_days_start_date_to_end_date) OVER w_part AS running_sum_running_max_days_start_date_to_end_date,
        MIN(running_max_days_start_date_to_end_date) OVER w_part AS running_min_running_max_days_start_date_to_end_date,
        COUNT(running_max_days_start_date_to_end_date) OVER w_part AS running_count_running_max_days_start_date_to_end_date,
        MIN(monthly_fee_avg_5) OVER w_recent AS end_date_year_min_monthly_fee_avg_5
    FROM ranked
    WINDOW w_part AS (PARTITION BY plan ORDER BY running_avg_days_start_date_to_end_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), w_recent AS (PARTITION BY YEAR(end_date))
)
-- Multi-level aggregation
SELECT
    DATE_TRUNC('WEEK', start_date) AS week_start,
    SUM(CASE WHEN plan = 'premium' THEN running_avg_days_start_date_to_end_date ELSE 0 END) AS premium_running_avg_days_start_date_to_end_date,
    COUNT(*) AS event_count,
    COUNT(*) FILTER (WHERE plan = 'standard') AS standard_rows,
    MAX(monthly_fee_avg_5) FILTER (WHERE plan = 'standard') AS standard_max_monthly_fee_avg_5,
    APPROX_PERCENTILE(end_date_year_min_monthly_fee_avg_5, ARRAY(0.25, 0.5, 0.75)) AS end_date_year_min_monthly_fee_avg_5_quartiles,
    COUNT_IF(plan = 'basic') AS basic_events,
    GROUPING(DATE_TRUNC('WEEK', start_date)) AS gid
FROM scored_subscriptions
GROUP BY ROLLUP (DATE_TRUNC('WEEK', start_date))
