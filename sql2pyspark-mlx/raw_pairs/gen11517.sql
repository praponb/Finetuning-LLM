WITH valid AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        DATEDIFF(end_date, start_date) AS days_start_date_to_end_date,
        QUARTER(end_date) AS end_date_quarter
    FROM subscriptions
    WHERE start_date >= DATE '2023-06-15'
),
latest AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM valid
    QUALIFY ROW_NUMBER() OVER (PARTITION BY plan ORDER BY end_date_quarter DESC NULLS FIRST, id) = 1
),
cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, CASE WHEN monthly_fee >= 203 THEN 'High' WHEN monthly_fee >= 98 THEN 'Medium' ELSE 'Low' END AS monthly_fee_band
    FROM latest
    WHERE customer_id IS NOT NULL
      AND days_start_date_to_end_date BETWEEN 23 AND 33
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        id,
        customer_id,
        plan,
        start_date,
        end_date,
        CUME_DIST() OVER (ORDER BY monthly_fee NULLS LAST) AS monthly_fee_cume_dist,
        LEAD(monthly_fee, 2) OVER (PARTITION BY DATE_TRUNC('YEAR', start_date), plan ORDER BY end_date_quarter, id) AS next_monthly_fee,
        AVG(days_start_date_to_end_date) OVER (ORDER BY days_start_date_to_end_date, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS days_start_date_to_end_date_avg_3
    FROM cleaned
    WHERE end_date_quarter = 52
),
scored AS (
    -- Running and moving aggregates
    SELECT
        id,
        customer_id,
        plan,
        monthly_fee_cume_dist,
        days_start_date_to_end_date_avg_3,
        AVG(next_monthly_fee) OVER (PARTITION BY customer_id ORDER BY start_date, id ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS next_monthly_fee_avg_5_prior,
        COUNT(monthly_fee_cume_dist) OVER w_part AS customer_id_count_monthly_fee_cume_dist,
        ROW_NUMBER() OVER (PARTITION BY plan ORDER BY end_date, id) AS row_seq
    FROM with_windows
    WINDOW w_part AS (PARTITION BY customer_id, plan)
),
totals AS (
    -- Aggregate with conditional metrics
    SELECT
        plan,
        customer_id,
        ROUND(SUM(monthly_fee_cume_dist) / NULLIF(COUNT(CASE WHEN plan = 'basic' THEN 1 END), 0), 2) AS monthly_fee_cume_dist_per_basic,
        AVG(row_seq) FILTER (WHERE plan = 'basic') AS basic_avg_row_seq,
        SUM(monthly_fee_cume_dist) FILTER (WHERE plan = 'enterprise') AS enterprise_total_monthly_fee_cume_dist
    FROM scored
    GROUP BY plan, customer_id
    HAVING AVG(row_seq) FILTER (WHERE plan = 'basic') > 0
)
SELECT
    plan,
    customer_id,
    monthly_fee_cume_dist_per_basic,
    basic_avg_row_seq
FROM totals
ORDER BY monthly_fee_cume_dist_per_basic;
