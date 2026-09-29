WITH joined AS (
    -- Attach customers attributes
    SELECT
        a.*,
        d.signup_date,
        d.country
    FROM subscriptions a
    JOIN customers d
        ON a.customer_id <=> d.id
    WHERE d.age < 31
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT
        *,
        DATEDIFF(CURRENT_DATE(), start_date) / 365.25 AS years_since_start_date,
        COALESCE(country, 'OTHER') AS country_clean
    FROM joined
    WHERE customer_id IS NOT NULL
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        customer_id,
        plan,
        start_date,
        monthly_fee,
        signup_date,
        years_since_start_date,
        country_clean,
        AVG(monthly_fee) OVER (PARTITION BY status, country ORDER BY monthly_fee, id ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS monthly_fee_avg_5,
        LAG(monthly_fee, 1) OVER (PARTITION BY status ORDER BY start_date, id) AS prev_monthly_fee,
        LAG(monthly_fee, 1) OVER (PARTITION BY MONTH(start_date), country ORDER BY monthly_fee, id) AS prev_monthly_fee_2,
        LEAD(years_since_start_date, 1, 0) OVER (PARTITION BY plan ORDER BY end_date, id) AS next_years_since_start_date,
        FIRST_VALUE(country) IGNORE NULLS OVER (PARTITION BY status ORDER BY monthly_fee, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_country
    FROM cleaned
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        id,
        plan,
        start_date,
        years_since_start_date,
        monthly_fee_avg_5,
        prev_monthly_fee_2,
        next_years_since_start_date,
        LEAD(monthly_fee_avg_5) OVER w_ordered AS next_monthly_fee_avg_5,
        LEAD(monthly_fee) OVER w_ordered AS next_monthly_fee,
        ROW_NUMBER() OVER w_ordered AS seq_no,
        AVG(prev_monthly_fee_2) OVER (PARTITION BY customer_id ORDER BY prev_monthly_fee, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS prev_monthly_fee_2_avg_6
    FROM with_windows
    WINDOW w_ordered AS (PARTITION BY customer_id ORDER BY signup_date, id)
),
enriched_subscriptions AS (
    -- Running and moving aggregates
    SELECT
        id,
        plan,
        start_date,
        years_since_start_date,
        monthly_fee_avg_5,
        prev_monthly_fee_2,
        next_years_since_start_date,
        next_monthly_fee_avg_5,
        LEAD(next_years_since_start_date) OVER w_grp AS next_next_years_since_start_date,
        LEAD(monthly_fee_avg_5) OVER w_grp AS next_monthly_fee_avg_5_2,
        ROW_NUMBER() OVER w_grp AS seq_no_2,
        LAG(next_monthly_fee_avg_5, 2, 0) OVER w_ordered AS prev_next_monthly_fee_avg_5
    FROM enriched
    WINDOW w_grp AS (PARTITION BY plan ORDER BY seq_no, id), w_ordered AS (PARTITION BY plan ORDER BY start_date, id)
)
-- Native SQL UNPIVOT (wide to long)
SELECT *
FROM (
    SELECT
        id,
        plan,
        next_years_since_start_date,
        next_next_years_since_start_date,
        prev_monthly_fee_2,
        years_since_start_date
    FROM enriched_subscriptions
) src
UNPIVOT (
    metric_value FOR metric_name IN (next_years_since_start_date, next_next_years_since_start_date, prev_monthly_fee_2, years_since_start_date)
) long_rows;
