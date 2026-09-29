WITH cleaned_tracks AS (
    -- Filter tracks
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        is_explicit,
        bpm
    FROM tracks
    WHERE UPPER(TRIM(title)) LIKE 'S%'
      AND NOT is_explicit
),
line_level AS (
    -- One row per play
    SELECT
        s.id,
        s.artist_id,
        s.duration_sec,
        s.is_explicit,
        s.bpm,
        p.id AS play_id,
        p.listener_id AS play_listener_id,
        p.played_at AS play_played_at,
        p.ms_played AS play_ms_played,
        p.device AS play_device
    FROM cleaned_tracks s
    JOIN plays p ON p.track_id = s.id AND p.device = 'tv'
),
ranked AS (
    -- Window metrics over several partitions
    SELECT
        *,
        NTILE(4) OVER (PARTITION BY play_listener_id, play_device ORDER BY play_ms_played DESC, id, play_id) AS play_ms_played_quartile,
        LAG(duration_sec) OVER (PARTITION BY play_listener_id, play_device ORDER BY play_ms_played DESC, id, play_id) AS prev_duration_sec
    FROM line_level
),
play_device_stats AS (
    -- Max bpm per play_device
    SELECT play_device, MAX(bpm) AS max_bpm
    FROM ranked
    GROUP BY play_device
),
vs_group AS (
    -- Rows compared with their group statistic
    SELECT s.*, g.max_bpm, ROUND(s.bpm - g.max_bpm, 2) AS bpm_minus_max
    FROM ranked s
    JOIN play_device_stats g ON g.play_device = s.play_device
    WHERE s.bpm = g.max_bpm
)
-- Final projection
SELECT
    id,
    artist_id,
    duration_sec,
    is_explicit,
    bpm,
    play_id,
    play_listener_id,
    play_played_at,
    play_ms_played,
    play_device,
    prev_duration_sec,
    bpm_minus_max
FROM vs_group
