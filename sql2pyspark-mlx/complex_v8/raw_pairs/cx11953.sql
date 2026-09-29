WITH recent AS (
    -- Scope the listeners rows
    SELECT
        id,
        plan,
        signup_date,
        birth_year,
        interests
    FROM listeners
    WHERE birth_year <= 1990
),
play_stats AS (
    -- Per-listener plays metrics
    SELECT
        listener_id,
        COUNT(*) AS play_count,
        ROUND(AVG(ms_played), 2) AS avg_play_ms_played,
        COUNT(*) AS play_count_2
    FROM plays
    GROUP BY listener_id
),
with_counts AS (
    -- Attach plays metrics
    SELECT
        s.*,
        COALESCE(p.play_count, 0) AS play_count,
        p.avg_play_ms_played,
        COALESCE(p.play_count_2, 0) AS play_count_2
    FROM recent s
    LEFT JOIN play_stats p ON p.listener_id = s.id
),
labeled AS (
    -- Label and score rows
    SELECT
        *,
        CASE WHEN plan IN ('premium', 'family') THEN 'group_a' WHEN plan = 'free' THEN 'group_b' ELSE 'other' END AS plan_group,
        DATE_FORMAT(signup_date, 'yyyy-MM-dd') AS signup_date_label,
        FLOOR(birth_year / 10) * 10 AS birth_year_bucket
    FROM with_counts
),
metrics AS (
    -- Previous / next values per key
    SELECT
        id,
        plan,
        signup_date,
        birth_year,
        interests,
        play_count_2,
        signup_date_label,
        birth_year_bucket,
        MAX(play_count) OVER w_play_count_2 AS play_count_2_max_play_count,
        MIN(birth_year_bucket) OVER (PARTITION BY play_count_2) AS play_count_2_min_birth_year_bucket
    FROM labeled
    WINDOW w_play_count_2 AS (PARTITION BY play_count_2)
),
screened AS (
    -- Compare against the overall min
    SELECT *
    FROM metrics
    WHERE play_count_2 > (SELECT MIN(play_count_2) FROM metrics)
),
annotated AS (
    -- Label and score rows
    SELECT
        id,
        signup_date,
        birth_year,
        interests,
        signup_date_label,
        play_count_2_min_birth_year_bucket,
        COALESCE(play_count_2, 0) AS play_count_2_filled
    FROM screened
),
with_activity AS (
    -- Rows with no matching plays
    SELECT s.*
    FROM annotated s
    WHERE NOT EXISTS (
            SELECT 1
            FROM plays p
            WHERE p.listener_id = s.id
              AND p.is_skipped = FALSE
        )
      AND s.signup_date >= DATE '2024-07-13'
)
-- Final projection
SELECT
    id,
    signup_date,
    interests,
    signup_date_label,
    play_count_2_filled,
    FLOOR(birth_year / 10) * 10 AS birth_year_bucket
FROM with_activity
ORDER BY id
LIMIT 5
