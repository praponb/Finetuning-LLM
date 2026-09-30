WITH recent AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        username,
        plan,
        signup_date,
        birth_year,
        interests
    FROM listeners
    WHERE birth_year BETWEEN 1966 AND 1981
),
filtered AS (
    -- Rows with no matching plays
    SELECT s.*
    FROM recent s
    WHERE NOT EXISTS (
            SELECT 1
            FROM plays p
            WHERE p.listener_id = s.id
              AND p.ms_played >= 157900
              AND p.device NOT IN ('phone', 'desktop')
        )
),
profiled AS (
    -- Attach plays metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FROM plays p WHERE p.listener_id = s.id) AS play_count,
        (SELECT MAX(p.played_at) FROM plays p WHERE p.listener_id = s.id) AS last_played_at,
        (SELECT COUNT(*) FROM plays p WHERE p.listener_id = s.id) AS play_count_2
    FROM filtered s
),
annotated AS (
    -- Label and score rows
    SELECT
        *,
        CASE WHEN plan IN ('student', 'free') THEN 'group_a' WHEN plan = 'premium' THEN 'group_b' ELSE 'other' END AS plan_group
    FROM profiled
)
-- Final projection
SELECT
    id,
    plan,
    signup_date,
    birth_year,
    interests,
    play_count_2,
    plan_group,
    play_count_2 + play_count AS play_count_2_plus_play_count
FROM annotated
ORDER BY plan_group, play_count_2_plus_play_count DESC NULLS FIRST
