WITH filtered_listeners AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        plan,
        country,
        signup_date,
        SUBSTRING(TRIM(country), 1, 2) AS country_prefix,
        DATE_TRUNC('YEAR', signup_date) AS signup_date_year,
        DATEDIFF(DATE '2025-12-31', signup_date) AS days_since_signup_date
    FROM listeners
    WHERE plan NOT IN ('premium', 'free')
      AND birth_year <= 1986
),
with_counts AS (
    -- Attach plays metrics
    SELECT
        s.*,
        (SELECT MIN(p.played_at) FROM plays p WHERE p.listener_id = s.id) AS first_played_at,
        (SELECT MAX(p.played_at) FROM plays p WHERE p.listener_id = s.id) AS last_played_at
    FROM filtered_listeners s
),
windowed AS (
    -- Previous / next values per key
    SELECT
        *,
        COUNT(*) OVER w_country_plan AS rows_in_country_plan,
        RANK() OVER (PARTITION BY MONTH(first_played_at), YEAR(signup_date_year) ORDER BY last_played_at DESC) AS last_played_at_rank
    FROM with_counts
    WINDOW w_country_plan AS (PARTITION BY country, plan)
),
play_stats_2 AS (
    -- Per-listener plays metrics
    SELECT
        listener_id,
        COUNT(CASE WHEN NOT is_skipped THEN 1 END) AS play_is_skipped_count
    FROM plays
    GROUP BY listener_id
),
summarized_rows AS (
    -- Attach plays metrics
    SELECT
        s.*,
        COALESCE(p.play_is_skipped_count, 0) AS play_is_skipped_count
    FROM windowed s
    LEFT JOIN play_stats_2 p ON p.listener_id = s.id
),
annotated AS (
    -- Derive helper columns
    SELECT
        *,
        COALESCE(rows_in_country_plan, 0) AS rows_in_country_plan_filled,
        first_played_at + INTERVAL 2 HOURS AS first_played_at_plus_2h,
        COALESCE(days_since_signup_date, 0) AS days_since_signup_date_filled
    FROM summarized_rows
    WHERE signup_date_year >= TIMESTAMP '2025-01-25 09:00:00'
)
SELECT
    id,
    plan,
    country,
    signup_date,
    country_prefix,
    signup_date_year,
    first_played_at,
    last_played_at_rank,
    first_played_at_plus_2h,
    days_since_signup_date_filled,
    DAYOFWEEK(signup_date) AS signup_date_dayofweek,
    DATEDIFF(DATE '2026-01-01', last_played_at) AS days_since_last_played_at
FROM annotated
ORDER BY last_played_at_rank DESC NULLS FIRST, plan
