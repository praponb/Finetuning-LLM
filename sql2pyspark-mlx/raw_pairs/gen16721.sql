WITH filtered AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        customer_id,
        plan,
        start_date,
        end_date,
        status,
        INITCAP(TRIM(status)) AS status_title,
        LENGTH(status) AS status_length
    FROM subscriptions
    WHERE monthly_fee BETWEEN 70 AND 130
      AND end_date > DATE '2023-06-27'
),
cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        plan,
        start_date,
        end_date,
        status,
        status_title,
        status_length,
        DATEDIFF(start_date, end_date) AS days_end_date_to_start_date,
        DATE_SUB(end_date, 67) AS end_date_minus_67d
    FROM filtered
    WHERE start_date >= DATE '2023-07-23'
      AND plan = 'basic'
),
enriched AS (
    -- Ranking and gap features
    SELECT
        *,
        AVG(days_end_date_to_start_date) OVER (PARTITION BY status ORDER BY days_end_date_to_start_date, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS days_end_date_to_start_date_avg_3,
        LAST_VALUE(days_end_date_to_start_date) IGNORE NULLS OVER (ORDER BY start_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_days_end_date_to_start_date,
        STDDEV_SAMP(status_length) OVER (PARTITION BY MONTH(end_date), plan) AS end_date_month_stddev_samp_status_length
    FROM cleaned
    WHERE days_end_date_to_start_date IS NOT NULL
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        plan,
        end_date,
        status_length,
        days_end_date_to_start_date,
        end_date_minus_67d,
        days_end_date_to_start_date_avg_3,
        last_known_days_end_date_to_start_date,
        COALESCE(status_length, 0) AS status_length_filled
    FROM enriched
    WHERE start_date > DATE '2023-09-14'
      AND MONTH(end_date) IN (4, 9, 12)
),
ranked AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM valid
    QUALIFY ROW_NUMBER() OVER (PARTITION BY plan ORDER BY end_date DESC NULLS FIRST, id) = 1
),
stats AS (
    -- Aggregate with conditional metrics
    SELECT
        CASE WHEN days_end_date_to_start_date >= 76 THEN 'LARGE' ELSE 'SMALL' END AS days_end_date_to_start_date_tier,
        plan,
        SUM(status_length_filled) FILTER (WHERE plan = 'premium') AS premium_total_status_length_filled,
        COUNT_IF(plan = 'premium') AS premium_events
    FROM ranked
    GROUP BY 1, 2
)
SELECT
    days_end_date_to_start_date_tier,
    plan,
    premium_events
FROM stats
ORDER BY premium_events DESC, days_end_date_to_start_date_tier
