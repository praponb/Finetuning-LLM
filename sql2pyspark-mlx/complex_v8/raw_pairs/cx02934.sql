WITH cleaned AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        username,
        plan,
        signup_date,
        birth_year,
        interests
    FROM listeners
    WHERE LENGTH(TRIM(username)) >= 5
      AND ARRAY_CONTAINS(interests, 'indie')
),
detail_rows AS (
    -- One row per play
    SELECT
        s.id,
        s.plan,
        s.signup_date,
        s.birth_year,
        s.interests,
        p.id AS play_id,
        p.device AS play_device,
        p.track_id AS play_track_id
    FROM cleaned s
    LEFT JOIN plays p ON p.listener_id = s.id
),
combined_details AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.plan,
        s.birth_year,
        s.interests,
        s.play_id,
        s.play_track_id,
        p.is_skipped AS play_is_skipped,
        p.played_at AS play_played_at,
        t.bpm AS track_bpm,
        t.duration_sec AS track_duration_sec
    FROM detail_rows s
    JOIN plays p ON p.id = s.play_id
    LEFT JOIN tracks t ON t.id = p.track_id
),
play_stats AS (
    -- Per-track plays metrics
    SELECT
        track_id,
        ROUND(AVG(ms_played), 2) AS avg_play_ms_played,
        COUNT(*) AS play_count
    FROM plays
    GROUP BY track_id
),
profiled AS (
    -- Attach plays metrics
    SELECT
        s.*,
        p.avg_play_ms_played,
        COALESCE(p.play_count, 0) AS play_count
    FROM combined_details s
    LEFT JOIN play_stats p ON p.track_id = s.play_track_id
)
-- Native SQL UNPIVOT (wide to long)
SELECT id, play_id, metric_name, reading
FROM (
    SELECT id, play_id, track_bpm, birth_year, play_count
    FROM profiled
) src
UNPIVOT (
    reading FOR metric_name IN (track_bpm AS m_track_bpm, birth_year AS birth_year_v, play_count AS m_play_count)
) unp
ORDER BY id, play_id, metric_name
