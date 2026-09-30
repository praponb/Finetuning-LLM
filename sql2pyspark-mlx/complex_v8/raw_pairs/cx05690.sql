WITH base_listeners AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        username,
        country,
        signup_date,
        DATEDIFF(DATE '2026-01-01', signup_date) AS days_since_signup_date,
        CASE WHEN birth_year >= 1990 THEN 'HIGH' WHEN birth_year >= 1970 THEN 'MEDIUM' ELSE 'LOW' END AS birth_year_tier,
        CASE WHEN country IN ('BR', 'JP') THEN 'group_a' WHEN country = 'UK' THEN 'group_b' ELSE 'other' END AS country_group
    FROM listeners
    WHERE signup_date >= DATE '2024-08-01'
),
windowed AS (
    -- Window metrics over several partitions
    SELECT
        id,
        username,
        country,
        signup_date,
        days_since_signup_date,
        birth_year_tier,
        ROUND(days_since_signup_date / NULLIF(SUM(days_since_signup_date) OVER w_country, 0), 4) AS days_since_signup_date_share_of_country,
        COUNT(*) OVER (PARTITION BY country) AS rows_in_country,
        NTH_VALUE(signup_date, 2) OVER (ORDER BY signup_date ASC NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_signup_date,
        MIN(days_since_signup_date) OVER (PARTITION BY country) AS country_min_days_since_signup_date,
        MAX(days_since_signup_date) OVER (ORDER BY signup_date ASC NULLS LAST, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS days_since_signup_date_max_6_prior
    FROM base_listeners
    WINDOW w_country AS (PARTITION BY country)
),
with_children AS (
    -- One row per play
    SELECT
        s.id,
        s.username,
        s.country,
        s.signup_date,
        s.days_since_signup_date,
        s.birth_year_tier,
        s.days_since_signup_date_share_of_country,
        s.rows_in_country,
        s.country_min_days_since_signup_date,
        s.days_since_signup_date_max_6_prior,
        p.id AS play_id,
        p.played_at AS play_played_at,
        p.is_skipped AS play_is_skipped,
        p.source AS play_source
    FROM windowed s
    LEFT JOIN plays p ON p.listener_id = s.id
),
latest_per_key AS (
    -- Keep the latest row per country
    SELECT
        id,
        username,
        country,
        signup_date,
        days_since_signup_date,
        birth_year_tier,
        days_since_signup_date_share_of_country,
        rows_in_country,
        country_min_days_since_signup_date,
        days_since_signup_date_max_6_prior,
        play_id,
        play_played_at,
        play_is_skipped,
        play_source
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY country ORDER BY signup_date DESC, id DESC, play_id DESC) AS rn
        FROM with_children
    ) ranked
    WHERE rn = 1
)
SELECT
    id,
    signup_date,
    days_since_signup_date,
    birth_year_tier,
    days_since_signup_date_share_of_country,
    rows_in_country,
    country_min_days_since_signup_date,
    play_id,
    play_played_at,
    play_is_skipped,
    play_source,
    COALESCE(play_source, 'UNKNOWN') AS play_source_or_unknown,
    LOWER(REGEXP_REPLACE(TRIM(country), ' ', '_')) AS country_slug
FROM latest_per_key
ORDER BY play_id NULLS LAST, id DESC
LIMIT 10
