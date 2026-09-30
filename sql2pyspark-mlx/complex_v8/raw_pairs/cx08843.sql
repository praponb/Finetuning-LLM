WITH prepared AS (
    -- Scope the tracks rows
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        release_date,
        is_explicit,
        CONCAT(title, '#', CAST(artist_id AS STRING)) AS title_ref,
        ABS(bpm - 100) AS bpm_distance,
        DATEDIFF(DATE '2025-06-30', release_date) AS days_since_release_date
    FROM tracks
),
artist_id_stats AS (
    -- Max bpm_distance per artist_id
    SELECT artist_id, MAX(bpm_distance) AS max_bpm_distance
    FROM prepared
    GROUP BY artist_id
),
vs_group AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.max_bpm_distance,
        ROUND(s.bpm_distance - g.max_bpm_distance, 2) AS bpm_distance_minus_max
    FROM prepared s
    JOIN artist_id_stats g ON g.artist_id = s.artist_id
    WHERE s.bpm_distance = g.max_bpm_distance
),
line_level AS (
    -- One row per play
    SELECT
        s.id,
        s.artist_id,
        s.title,
        s.duration_sec,
        s.release_date,
        s.bpm_distance,
        s.days_since_release_date,
        s.max_bpm_distance,
        p.id AS play_id,
        p.device AS play_device,
        p.is_skipped AS play_is_skipped
    FROM vs_group s
    JOIN plays p ON p.track_id = s.id
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT play_is_skipped, play_id, play_device, duration_sec
    FROM line_level
) src
PIVOT (
    SUM(duration_sec) AS total, MAX(duration_sec) AS max_v
    FOR play_device IN ('speaker', 'phone')
)
