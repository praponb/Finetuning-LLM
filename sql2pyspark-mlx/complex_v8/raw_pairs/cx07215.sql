WITH base AS (
    -- Filter listeners
    SELECT
        id,
        username,
        plan,
        country,
        signup_date,
        birth_year,
        interests,
        LOWER(REGEXP_REPLACE(TRIM(plan), ' ', '_')) AS plan_slug,
        DATE_TRUNC('YEAR', signup_date) AS signup_date_year,
        COALESCE(birth_year, 0) AS birth_year_filled
    FROM listeners
    WHERE plan = 'family'
),
profiled AS (
    -- Attach plays metrics
    SELECT
        s.*,
        (SELECT MAX(p.played_at) FROM plays p WHERE p.listener_id = s.id) AS last_played_at,
        (SELECT COUNT(CASE WHEN p.is_skipped = TRUE THEN 1 END) FROM plays p WHERE p.listener_id = s.id) AS play_is_skipped_count,
        (SELECT COUNT(*) FROM plays p WHERE p.listener_id = s.id) AS play_count
    FROM base s
),
stats AS (
    -- Aggregate with conditional metrics
    SELECT
        YEAR(signup_date_year) AS signup_date_year_year,
        MIN(birth_year) AS min_birth_year,
        COUNT(DISTINCT id) AS distinct_ids,
        ARRAY_SORT(COLLECT_SET(username)) AS username_list,
        MAX(play_is_skipped_count) AS max_play_is_skipped_count,
        MAX(play_count) AS max_play_count
    FROM profiled
    GROUP BY YEAR(signup_date_year)
),
filtered AS (
    -- Compare against the overall min
    SELECT *
    FROM stats
    WHERE max_play_is_skipped_count > (SELECT MIN(max_play_is_skipped_count) FROM stats)
)
SELECT
    signup_date_year_year,
    min_birth_year,
    distinct_ids,
    username_list,
    max_play_is_skipped_count
FROM filtered
ORDER BY min_birth_year, max_play_is_skipped_count NULLS LAST
