WITH cleaned AS (
    -- Scope the listeners rows
    SELECT
        id,
        username,
        country,
        signup_date,
        interests,
        CASE WHEN plan IN ('family', 'student') THEN 'group_a' WHEN plan = 'free' THEN 'group_b' ELSE 'other' END AS plan_group
    FROM listeners
    WHERE plan = 'student'
),
play_stats AS (
    -- Per-listener plays metrics
    SELECT listener_id, MIN(played_at) AS first_played_at, COUNT(*) AS play_count
    FROM plays
    GROUP BY listener_id
),
with_counts AS (
    -- Attach plays metrics
    SELECT
        s.*,
        p.first_played_at,
        COALESCE(p.play_count, 0) AS play_count
    FROM cleaned s
    LEFT JOIN play_stats p ON p.listener_id = s.id
),
derived AS (
    -- Derive helper columns
    SELECT
        *,
        COALESCE(play_count, 0) AS play_count_filled,
        COALESCE(play_count, 0) AS play_count_filled_2,
        CONCAT_WS(' / ', username, country) AS username_country_label
    FROM with_counts
    WHERE play_count >= 2
)
SELECT
    play_count_filled,
    COALESCE(plan_group, 'ALL') AS plan_group,
    play_count,
    GROUPING(play_count_filled) AS is_total_play_count_filled,
    COUNT(DISTINCT plan_group) AS distinct_plan_groups,
    MIN(signup_date) AS first_signup_date
FROM derived
GROUP BY GROUPING SETS ((play_count_filled, plan_group, play_count), (play_count_filled))
ORDER BY play_count_filled NULLS LAST, plan_group NULLS LAST, play_count NULLS LAST
