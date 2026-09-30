WITH filtered_tracks AS (
    -- Keep valid rows and derive helper columns
    SELECT *
    FROM tracks
    WHERE (UPPER(TRIM(title)) LIKE 'E%' OR bpm < 163)
),
filtered AS (
    -- Keep rows whose artist matches
    SELECT s.*
    FROM filtered_tracks s
    WHERE NOT EXISTS (
            SELECT 1
            FROM artists a
            WHERE a.id = s.artist_id
              AND a.genre = 'Indie'
        )
),
with_children_plays AS (
    -- One row per play
    SELECT
        s.id,
        s.artist_id,
        s.bpm,
        s.moods,
        p.id AS play_id,
        p.device AS play_device,
        p.listener_id AS play_listener_id,
        p.ms_played AS play_ms_played,
        p.is_skipped AS play_is_skipped
    FROM filtered s
    JOIN plays p ON p.track_id = s.id
),
play_device_stats AS (
    -- Avg play_ms_played per play_device
    SELECT play_device, AVG(play_ms_played) AS avg_play_ms_played
    FROM with_children_plays
    GROUP BY play_device
),
outliers AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_play_ms_played,
        ROUND(s.play_ms_played - g.avg_play_ms_played, 2) AS play_ms_played_minus_avg
    FROM with_children_plays s
    JOIN play_device_stats g ON g.play_device = s.play_device
    WHERE s.play_ms_played > g.avg_play_ms_played
),
with_details AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.artist_id,
        s.moods,
        s.play_id,
        s.play_device,
        s.play_listener_id,
        s.play_ms_played_minus_avg,
        a.artist_name,
        p.source AS play_source
    FROM outliers s
    JOIN artists a ON a.id = s.artist_id
    JOIN plays p ON p.id = s.play_id
),
combined_details AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.artist_id,
        s.moods,
        s.play_id,
        s.play_device,
        s.play_listener_id,
        s.play_ms_played_minus_avg,
        s.artist_name,
        s.play_source,
        p.device AS play_device_2
    FROM with_details s
    LEFT JOIN plays p ON p.id = s.play_id
)
-- Final projection
SELECT
    id,
    artist_id,
    moods,
    play_device,
    play_listener_id,
    play_ms_played_minus_avg,
    play_source,
    play_device_2,
    CONCAT(play_source, '#', CAST(id AS STRING)) AS play_source_ref
FROM combined_details
ORDER BY play_source, id NULLS LAST
