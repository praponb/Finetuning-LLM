WITH prepared AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        username,
        plan,
        birth_year,
        interests
    FROM listeners
),
scored AS (
    -- Label and score rows
    SELECT
        *,
        FLOOR(birth_year / 10) * 10 AS birth_year_bucket,
        CONCAT_WS(' / ', username, plan) AS username_plan_label,
        COALESCE(birth_year, 0) AS birth_year_filled
    FROM prepared
    WHERE ARRAY_CONTAINS(interests, 'jazz')
),
derived AS (
    -- Label and score rows
    SELECT
        *,
        ROUND(birth_year_filled / NULLIF(birth_year, 0), 3) AS birth_year_filled_per_birth_year,
        CONCAT_WS(' / ', username, plan) AS username_plan_label_2,
        COALESCE(birth_year, 0) AS birth_year_filled_2
    FROM scored
),
play_stats AS (
    -- Per-listener plays metrics
    SELECT
        listener_id,
        COUNT(*) AS play_count,
        MIN(played_at) AS first_played_at,
        COUNT(CASE WHEN device IS NOT NULL THEN 1 END) AS play_device_count
    FROM plays
    GROUP BY listener_id
),
with_stats AS (
    -- Attach plays metrics
    SELECT
        s.*,
        COALESCE(p.play_count, 0) AS play_count,
        p.first_played_at,
        COALESCE(p.play_device_count, 0) AS play_device_count
    FROM derived s
    LEFT JOIN play_stats p ON p.listener_id = s.id
)
-- Final projection
SELECT
    id,
    username,
    plan,
    birth_year_bucket,
    username_plan_label,
    birth_year_filled_per_birth_year,
    username_plan_label_2,
    birth_year_filled_2,
    play_count,
    first_played_at,
    play_device_count,
    birth_year - play_count AS birth_year_minus_play_count,
    FLOOR(birth_year_filled / 10) * 10 AS birth_year_filled_bucket
FROM with_stats
ORDER BY username_plan_label_2, id
LIMIT 5
