WITH recent_tracks AS (
    -- Filter tracks
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        release_date,
        is_explicit,
        moods
    FROM tracks
    WHERE title IS NOT NULL
      AND bpm > 94
),
metrics AS (
    -- Previous / next values per key
    SELECT
        *,
        ROUND(duration_sec / NULLIF(SUM(duration_sec) OVER w_artist_id, 0), 4) AS duration_sec_share_of_artist_id,
        ROW_NUMBER() OVER (PARTITION BY is_explicit, artist_id ORDER BY duration_sec, id) AS duration_sec_seq
    FROM recent_tracks
    WINDOW w_artist_id AS (PARTITION BY artist_id)
),
line_level AS (
    -- One row per play
    SELECT
        s.id,
        s.artist_id,
        s.title,
        s.duration_sec,
        s.duration_sec_share_of_artist_id,
        s.duration_sec_seq,
        p.id AS play_id,
        p.ms_played AS play_ms_played,
        p.played_at AS play_played_at
    FROM metrics s
    JOIN plays p ON p.track_id = s.id AND p.played_at < TIMESTAMP '2025-03-02 11:00:00'
)
-- Final projection
SELECT DISTINCT duration_sec_seq
FROM line_level
WHERE play_ms_played BETWEEN 67932 AND 359437
ORDER BY duration_sec_seq DESC
