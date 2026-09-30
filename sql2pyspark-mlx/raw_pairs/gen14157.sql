WITH valid AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        customer_id,
        plan,
        end_date,
        monthly_fee,
        DATEDIFF(CURRENT_DATE(), end_date) AS days_since_end_date
    FROM subscriptions
    WHERE start_date >= TO_DATE('2023-05-30')
      AND customer_id IS NOT NULL
),
ranked AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY plan ORDER BY days_since_end_date DESC NULLS FIRST, id ASC) AS rank_in_group
        FROM valid
    ) t
    WHERE rank_in_group = 1
),
ranked_subscriptions AS (
    -- Keep one row per key (latest first)
    SELECT
        id,
        monthly_fee,
        days_since_end_date,
        rank_in_group
    FROM ranked
    QUALIFY RANK() OVER (PARTITION BY plan ORDER BY days_since_end_date DESC) = 1
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        rank_in_group,
        AVG(days_since_end_date) OVER w_hist AS days_since_end_date_avg_3,
        FIRST_VALUE(days_since_end_date) IGNORE NULLS OVER (ORDER BY days_since_end_date ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_days_since_end_date,
        LEAD(monthly_fee, 1) OVER (ORDER BY rank_in_group ASC, id ASC) AS next_monthly_fee
    FROM ranked_subscriptions
    WINDOW w_hist AS (ORDER BY days_since_end_date ASC, id ASC ROWS BETWEEN 3 PRECEDING AND CURRENT ROW)
)
-- Grouped metrics
SELECT
    CASE WHEN days_since_end_date_avg_3 >= 88 THEN 'HIGH' ELSE 'LOW' END AS days_since_end_date_avg_3_tier,
    PERCENTILE_APPROX(days_since_end_date_avg_3, ARRAY(0.25, 0.5, 0.75)) AS days_since_end_date_avg_3_quartiles,
    SUM(next_monthly_fee) AS total_next_monthly_fee,
    COUNT(*) AS num_rows
FROM metrics
GROUP BY CUBE (CASE WHEN days_since_end_date_avg_3 >= 88 THEN 'HIGH' ELSE 'LOW' END)
ORDER BY num_rows DESC
