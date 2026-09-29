WITH prepared_plays AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        device,
        is_skipped,
        source,
        SUBSTRING(TRIM(source), 1, 4) AS source_prefix,
        INITCAP(TRIM(source)) AS source_clean,
        DATE_TRUNC('HOUR', played_at) AS played_at_hour
    FROM plays
),
with_details AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.played_at,
        s.ms_played,
        s.device,
        s.is_skipped,
        s.source,
        s.source_prefix,
        s.source_clean,
        s.played_at_hour,
        t.title AS track_title,
        l.username AS listener_username
    FROM prepared_plays s
    JOIN tracks t ON t.id = s.track_id
    JOIN listeners l ON l.id = s.listener_id
),
line_level AS (
    -- One row per play
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.played_at,
        s.ms_played,
        s.is_skipped,
        s.source,
        s.source_prefix,
        s.played_at_hour,
        s.track_title,
        p.id AS play_id,
        p.source AS play_source,
        p.device AS play_device,
        p.played_at AS play_played_at,
        p.is_skipped AS play_is_skipped
    FROM with_details s
    JOIN plays p ON p.listener_id = s.listener_id
),
with_stats AS (
    -- Attach plays metrics
    SELECT
        s.*,
        (SELECT SUM(p.ms_played) FROM plays p WHERE p.track_id = s.track_id) AS total_play_ms_played
    FROM line_level s
)
-- Subtotals with GROUPING SETS
SELECT
    play_id,
    track_id,
    COALESCE(play_source, 'ALL') AS play_source,
    ROUND(SUM(total_play_ms_played) / NULLIF(SUM(ms_played), 0), 3) AS total_play_ms_played_to_ms_played_ratio,
    COUNT(*) AS n_rows,
    MAX(ms_played) AS max_ms_played
FROM with_stats
GROUP BY GROUPING SETS ((play_id, track_id, play_source), (play_id), ())
ORDER BY play_id NULLS LAST, track_id NULLS LAST, play_source NULLS LAST
