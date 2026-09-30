WITH filtered_listeners AS (
    -- Scope the listeners rows
    SELECT
        id,
        country,
        signup_date,
        interests,
        SIZE(interests) AS interests_count,
        LENGTH(TRIM(country)) AS country_len
    FROM listeners
    WHERE country = 'JP'
),
metrics AS (
    -- Previous / next values per key
    SELECT
        *,
        ROUND(AVG(country_len) OVER (PARTITION BY interests_count ORDER BY signup_date, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING), 2) AS country_len_avg_3_prior,
        SUM(country_len) OVER (PARTITION BY country_len, YEAR(signup_date)) AS country_len_signup_date_year_sum_country_len,
        ROUND(PERCENT_RANK() OVER (PARTITION BY interests_count, country_len ORDER BY interests_count, id), 3) AS interests_count_percent_rank
    FROM filtered_listeners
),
with_trends AS (
    -- Window metrics over several partitions
    SELECT
        id,
        country,
        interests_count,
        country_len,
        country_len_avg_3_prior,
        country_len_signup_date_year_sum_country_len,
        interests_count_percent_rank,
        COUNT(*) OVER (PARTITION BY country, interests_count) AS rows_in_country_interests_count,
        LAST_VALUE(country_len) OVER (ORDER BY interests_count, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_country_len
    FROM metrics
)
SELECT
    id,
    country,
    country_len_avg_3_prior,
    interests_count_percent_rank,
    rows_in_country_interests_count,
    last_country_len
FROM with_trends
ORDER BY rows_in_country_interests_count NULLS LAST, country DESC
