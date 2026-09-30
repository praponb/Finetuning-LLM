WITH valid AS (
    -- Scope the tracks rows
    SELECT
        id,
        artist_id,
        title,
        is_explicit,
        bpm
    FROM tracks
    WHERE release_date >= DATE '2024-07-01'
      AND bpm < 155
),
detail_rows AS (
    -- One row per play
    SELECT
        s.id,
        s.artist_id,
        s.is_explicit,
        p.id AS play_id,
        p.ms_played AS play_ms_played,
        p.listener_id AS play_listener_id
    FROM valid s
    JOIN plays p ON p.track_id = s.id
)
-- Subtotals with GROUPING SETS
SELECT
    artist_id,
    play_listener_id,
    is_explicit,
    SUM(play_ms_played) FILTER (WHERE is_explicit = TRUE) AS play_ms_played_is_explicit_true,
    ROUND(STDDEV(play_ms_played), 2) AS stddev_play_ms_played,
    COUNT(*) AS n_rows
FROM detail_rows
GROUP BY GROUPING SETS ((artist_id, play_listener_id, is_explicit), (artist_id), ())
ORDER BY artist_id NULLS LAST, play_listener_id NULLS LAST, is_explicit NULLS LAST
