WITH base AS (
    -- Scope the listeners rows
    SELECT
        id,
        username,
        country,
        signup_date,
        UPPER(TRIM(plan)) AS plan_clean
    FROM listeners
    WHERE plan IN ('premium', 'student', 'free')
),
unique_rows AS (
    -- Keep the latest row per country
    SELECT
        id,
        username,
        country,
        signup_date,
        plan_clean
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY country ORDER BY signup_date DESC, id DESC) AS recency_rank
        FROM base
    ) ranked
    WHERE recency_rank = 1
),
grouped AS (
    -- Summarise per group
    SELECT
        YEAR(signup_date) AS signup_date_year,
        country,
        COUNT(*) FILTER (WHERE country NOT IN ('KR', 'UK')) AS n_country_not_in_kr_uk,
        MAX(signup_date) AS last_signup_date,
        COUNT(*) AS cnt
    FROM unique_rows
    GROUP BY YEAR(signup_date), country
),
scored AS (
    -- Derive helper columns
    SELECT
        *,
        CASE WHEN signup_date_year >= 2025 THEN 'large' WHEN signup_date_year >= 2024 THEN 'medium' ELSE 'small' END AS signup_date_year_tier
    FROM grouped
),
qualified AS (
    -- Compare against the overall avg
    SELECT *
    FROM scored
    WHERE cnt > (SELECT AVG(cnt) FROM scored)
),
derived AS (
    -- Derive helper columns
    SELECT
        signup_date_year,
        country,
        cnt,
        signup_date_year_tier,
        GREATEST(signup_date_year, cnt) AS greatest_signup_date_year_cnt,
        ADD_MONTHS(last_signup_date, 12) AS last_signup_date_plus_12m
    FROM qualified
)
-- Final projection
SELECT
    signup_date_year,
    country,
    cnt,
    signup_date_year_tier,
    last_signup_date_plus_12m
FROM derived
WHERE signup_date_year < 2025
ORDER BY signup_date_year_tier, country DESC
