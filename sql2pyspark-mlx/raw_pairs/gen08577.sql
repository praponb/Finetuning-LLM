WITH filtered AS (
    -- Keep recent, valid rows
    SELECT
        id,
        name,
        country,
        segment,
        signup_date,
        is_active,
        age,
        LENGTH(segment) AS segment_length,
        COALESCE(segment, 'NONE') AS segment_filled
    FROM customers
    WHERE city IS NOT NULL
      AND signup_date >= '2025-08-25'
),
valid AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        TRIM(country) AS clean_country,
        ADD_MONTHS(signup_date, 6) AS signup_date_plus_months,
        COALESCE(segment, 'DIRECT') AS resolved_segment
    FROM filtered
    WHERE signup_date > DATE '2023-08-04'
      AND age >= 56
),
metrics AS (
    -- Ranking and gap features
    SELECT
        *,
        FIRST_VALUE(segment_length, TRUE) OVER (PARTITION BY country ORDER BY signup_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_segment_length,
        RANK() OVER (PARTITION BY DATE_TRUNC('month', signup_date_plus_months), country ORDER BY segment_length NULLS LAST) AS segment_length_rank,
        AVG(age) OVER (PARTITION BY country ORDER BY signup_date_plus_months, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS age_avg_3,
        LEAD(segment_length, 1) OVER (PARTITION BY segment ORDER BY age, id) AS next_segment_length
    FROM valid
    WHERE segment IN ('Corporate', 'Consumer', 'SMB')
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT *, ADD_MONTHS(signup_date_plus_months, 1) AS signup_date_plus_months_plus_months
    FROM metrics
    WHERE country IN ('US', 'JP')
),
cleaned_customers AS (
    -- Keep recent, valid rows
    SELECT
        *,
        ROUND(first_known_segment_length / age, 2) AS first_known_segment_length_per_age,
        COALESCE(segment, 'N/A') AS segment_filled_2
    FROM cleaned
    WHERE signup_date > DATE '2023-04-25'
      AND age > 63
)
-- Aggregate with conditional metrics
SELECT
    DATE_TRUNC('QUARTER', signup_date) AS quarter_start,
    CASE WHEN segment_length >= 74 THEN 'PREMIUM' ELSE 'STANDARD' END AS segment_length_tier,
    COUNT(*) FILTER (WHERE country = 'JP') AS jp_rows,
    COUNT(DISTINCT id) AS distinct_ids,
    AVG(first_known_segment_length_per_age) AS avg_first_known_segment_length_per_age,
    SIZE(COLLECT_SET(segment)) AS num_segments,
    MAX(age_avg_3) FILTER (WHERE segment = 'SMB') AS smb_max_age_avg_3,
    GROUPING(DATE_TRUNC('QUARTER', signup_date)) AS grouping_level
FROM cleaned_customers
GROUP BY GROUPING SETS ((DATE_TRUNC('QUARTER', signup_date), CASE WHEN segment_length >= 74 THEN 'PREMIUM' ELSE 'STANDARD' END), (DATE_TRUNC('QUARTER', signup_date)), ())
ORDER BY quarter_start NULLS FIRST
