WITH filtered_listeners AS (
    -- Scope the listeners rows
    SELECT
        id,
        username,
        plan,
        birth_year
    FROM listeners
    WHERE birth_year > 1967
      AND plan = 'premium'
),
labeled AS (
    -- Add calculated columns
    SELECT
        *,
        FLOOR(birth_year / 10) * 10 AS birth_year_bucket,
        LENGTH(TRIM(plan)) AS plan_len,
        SUBSTRING(TRIM(plan), 1, 3) AS plan_prefix
    FROM filtered_listeners
    WHERE username IS NOT NULL
),
play_stats AS (
    -- Per-listener plays metrics
    SELECT listener_id, COUNT(*) AS play_count
    FROM plays
    GROUP BY listener_id
),
with_stats AS (
    -- Attach plays metrics
    SELECT
        s.*,
        COALESCE(p.play_count, 0) AS play_count
    FROM labeled s
    LEFT JOIN play_stats p ON p.listener_id = s.id
),
with_activity AS (
    -- Compare against the overall min
    SELECT *
    FROM with_stats
    WHERE play_count > (SELECT MIN(play_count) FROM with_stats)
),
with_children AS (
    -- One row per play
    SELECT
        s.id,
        s.username,
        s.plan,
        s.birth_year,
        s.birth_year_bucket,
        s.plan_prefix,
        s.play_count,
        p.id AS play_id,
        p.played_at AS play_played_at,
        p.device AS play_device,
        p.is_skipped AS play_is_skipped
    FROM with_activity s
    JOIN plays p ON p.listener_id = s.id AND p.played_at < TIMESTAMP '2025-02-24 11:00:00'
),
scored AS (
    -- Derive helper columns
    SELECT
        id,
        username,
        plan,
        birth_year,
        birth_year_bucket,
        play_id,
        play_played_at,
        play_device,
        UPPER(TRIM(play_device)) AS play_device_clean,
        GREATEST(birth_year, birth_year_bucket) AS greatest_birth_year_birth_year_bucket
    FROM with_children
    WHERE play_played_at >= TIMESTAMP '2025-02-06 12:00:00'
),
without_matches AS (
    -- Rows that have matching plays
    SELECT s.*
    FROM scored s
    WHERE s.id IN (
            SELECT p.listener_id
            FROM plays p
            WHERE HOUR(p.played_at) BETWEEN 7 AND 12
              AND p.is_skipped = TRUE
        )
)
SELECT
    play_id,
    plan,
    play_device,
    SUM(CASE WHEN play_device = 'phone' THEN 1 ELSE 0 END) AS n_play_device_phone,
    SUM(birth_year_bucket) FILTER (WHERE plan IN ('free', 'premium', 'family')) AS birth_year_bucket_plan_in_free_premium_family
FROM without_matches
GROUP BY CUBE(play_id, plan, play_device)
