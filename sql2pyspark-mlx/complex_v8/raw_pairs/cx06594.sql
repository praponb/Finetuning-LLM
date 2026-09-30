WITH scoped_listeners AS (
    -- Scope the listeners rows
    SELECT
        id,
        username,
        plan,
        country,
        signup_date,
        birth_year,
        CONCAT(country, '#', CAST(birth_year AS STRING)) AS country_ref,
        DATEDIFF(DATE '2026-01-01', signup_date) AS days_since_signup_date
    FROM listeners
    WHERE (signup_date >= DATE '2024-10-17' OR UPPER(TRIM(username)) LIKE 'H%')
),
annotated AS (
    -- Derive helper columns
    SELECT
        *,
        ROUND(birth_year / NULLIF(days_since_signup_date, 0), 3) AS birth_year_per_days_since_signup_date
    FROM scoped_listeners
),
exploded_detail AS (
    -- One row per play
    SELECT
        s.id,
        s.plan,
        s.country,
        s.signup_date,
        s.country_ref,
        p.id AS play_id,
        p.track_id AS play_track_id,
        p.played_at AS play_played_at,
        p.device AS play_device
    FROM annotated s
    LEFT JOIN plays p ON p.listener_id = s.id
),
enriched AS (
    -- Derive helper columns
    SELECT
        id,
        plan,
        country,
        play_id,
        play_track_id,
        play_played_at,
        play_device,
        MONTH(signup_date) AS signup_date_month,
        TO_DATE(play_played_at) AS play_played_at_date
    FROM exploded_detail
    WHERE country IN ('BR', 'UK', 'KR')
),
unique_rows AS (
    -- Keep the earliest row per plan, play_device
    SELECT
        id,
        plan,
        country,
        play_id,
        play_track_id,
        play_played_at,
        play_device,
        signup_date_month,
        play_played_at_date
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY plan, play_device ORDER BY play_played_at_date, id, play_id) AS row_num
        FROM enriched
    ) ranked
    WHERE row_num = 1
),
filtered AS (
    -- Compare against the overall avg
    SELECT *
    FROM unique_rows
    WHERE signup_date_month > (SELECT AVG(signup_date_month) FROM unique_rows)
),
unique_rows_2 AS (
    -- Keep the earliest row per country, play_track_id
    SELECT
        id,
        plan,
        country,
        play_id,
        play_track_id,
        play_played_at,
        play_device,
        signup_date_month,
        play_played_at_date
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY country, play_track_id ORDER BY play_played_at, id, play_id) AS row_num
        FROM filtered
    ) ranked
    WHERE row_num = 1
)
-- Final projection
SELECT
    id,
    plan,
    country,
    play_id,
    play_track_id,
    play_device,
    signup_date_month
FROM unique_rows_2
ORDER BY play_track_id NULLS LAST, country, id, play_id
LIMIT 5
