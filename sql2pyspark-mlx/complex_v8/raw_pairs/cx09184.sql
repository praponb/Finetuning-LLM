WITH scoped_listeners AS (
    -- Filter listeners
    SELECT
        id,
        username,
        country,
        signup_date,
        birth_year,
        LOWER(TRIM(country)) AS country_clean,
        DATE_FORMAT(signup_date, 'MMM yyyy') AS signup_date_label
    FROM listeners
),
without_matches AS (
    -- Rows with no matching plays
    SELECT s.*
    FROM scoped_listeners s
    WHERE NOT EXISTS (
            SELECT 1
            FROM plays p
            WHERE p.listener_id = s.id
              AND p.device NOT IN ('phone', 'tv')
        )
),
stats AS (
    -- Summarise per group
    SELECT
        country,
        COUNT(DISTINCT id) AS distinct_ids,
        DATEDIFF(MAX(signup_date), MIN(signup_date)) AS signup_date_span_days,
        SUM(CASE WHEN country IS NOT NULL THEN 1 ELSE 0 END) AS n_country_is_not_null,
        MAX(birth_year) AS max_birth_year
    FROM without_matches
    GROUP BY 1
),
metrics AS (
    -- Window metrics over several partitions
    SELECT
        *,
        SUM(n_country_is_not_null) OVER (PARTITION BY n_country_is_not_null, country) AS n_country_is_not_null_country_sum_n_country_is_n,
        ROUND(max_birth_year / NULLIF(SUM(max_birth_year) OVER (PARTITION BY n_country_is_not_null, country), 0), 4) AS max_birth_year_share_of_n_country_is_not_null_co,
        LAG(max_birth_year, 3) OVER (ORDER BY distinct_ids, country) AS prev3_max_birth_year,
        DENSE_RANK() OVER (PARTITION BY country, distinct_ids ORDER BY signup_date_span_days) AS signup_date_span_days_dense_rank
    FROM stats
),
annotated AS (
    -- Add calculated columns
    SELECT
        *,
        CASE WHEN country IN ('KR', 'UK') THEN 'group_a' WHEN country = 'JP' THEN 'group_b' ELSE 'other' END AS country_group,
        SUBSTRING(TRIM(country), 1, 4) AS country_prefix
    FROM metrics
    WHERE country IN ('JP', 'BR')
),
with_windows AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        LAG(signup_date_span_days, 3) OVER (ORDER BY prev3_max_birth_year ASC NULLS LAST, country) AS prev3_signup_date_span_days,
        LEAD(distinct_ids) OVER w_all_by_prev3_max_birth_year AS next_distinct_ids,
        ROUND(PERCENT_RANK() OVER (ORDER BY prev3_max_birth_year ASC NULLS LAST, country), 3) AS prev3_max_birth_year_percent_rank
    FROM annotated
    WINDOW w_all_by_prev3_max_birth_year AS (ORDER BY prev3_max_birth_year ASC NULLS LAST, country)
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT country_group, country, prev3_signup_date_span_days
    FROM with_windows
) src
PIVOT (
    MIN(prev3_signup_date_span_days)
    FOR country IN ('JP', 'TH', 'KR')
)
ORDER BY country_group
