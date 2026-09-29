WITH valid AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        artist_id,
        duration_sec,
        release_date,
        bpm,
        moods
    FROM tracks
    WHERE title IS NOT NULL
),
with_windows AS (
    -- Previous / next values per key
    SELECT
        id,
        duration_sec,
        release_date,
        bpm,
        moods,
        MAX(duration_sec) OVER (PARTITION BY artist_id) AS artist_id_max_duration_sec,
        MAX(bpm) OVER (PARTITION BY artist_id ORDER BY bpm DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_bpm_so_far
    FROM valid
),
line_level AS (
    -- One row per play
    SELECT
        s.id,
        s.duration_sec,
        s.moods,
        s.artist_id_max_duration_sec,
        s.max_bpm_so_far,
        p.id AS play_id,
        p.ms_played AS play_ms_played,
        p.listener_id AS play_listener_id
    FROM with_windows s
    JOIN plays p ON p.track_id = s.id AND p.ms_played >= 73625
)
SELECT
    play_listener_id,
    play_id,
    ROUND(STDDEV(duration_sec), 2) AS stddev_duration_sec,
    COUNT(*) AS cnt
FROM line_level
GROUP BY GROUPING SETS ((play_listener_id, play_id), (play_listener_id), ())
