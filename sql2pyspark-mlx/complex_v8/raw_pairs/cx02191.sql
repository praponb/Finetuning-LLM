WITH prepared_listeners AS (
    -- Scope the listeners rows
    SELECT
        id,
        plan,
        signup_date,
        birth_year
    FROM listeners
    WHERE birth_year < 1980
      AND LOWER(username) LIKE '%is%'
),
annotated AS (
    -- Derive helper columns
    SELECT
        *,
        FLOOR(birth_year / 10) * 10 AS birth_year_bucket,
        COALESCE(birth_year, 0) AS birth_year_filled
    FROM prepared_listeners
    WHERE plan <> 'free'
),
labeled AS (
    -- Add calculated columns
    SELECT
        id,
        plan,
        birth_year,
        LENGTH(TRIM(plan)) AS plan_len,
        CONCAT(plan, '#', CAST(birth_year_filled AS STRING)) AS plan_ref
    FROM annotated
    WHERE birth_year <= 1994
),
with_children_plays AS (
    -- One row per play
    SELECT
        s.id,
        s.birth_year,
        s.plan_len,
        p.id AS play_id,
        p.device AS play_device,
        p.source AS play_source,
        p.ms_played AS play_ms_played,
        p.is_skipped AS play_is_skipped
    FROM labeled s
    JOIN plays p ON p.listener_id = s.id AND p.ms_played <= 251066
)
-- Subtotals with ROLLUP
SELECT
    play_id,
    play_is_skipped,
    GROUPING_ID() AS grouping_level,
    ROUND(SUM(play_ms_played) / NULLIF(SUM(birth_year), 0), 3) AS play_ms_played_to_birth_year_ratio,
    SUM(CASE WHEN play_is_skipped = FALSE THEN birth_year ELSE 0 END) AS birth_year_play_is_skipped_false
FROM with_children_plays
GROUP BY ROLLUP(play_id, play_is_skipped)
ORDER BY play_id NULLS LAST, play_is_skipped NULLS LAST
