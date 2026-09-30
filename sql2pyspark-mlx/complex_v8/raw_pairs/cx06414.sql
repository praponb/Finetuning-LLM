WITH combined AS (
    -- Stack the branches
    SELECT bpm, artist_id AS qty, 'track' AS origin
    FROM tracks
    WHERE artist_id >= 11
    UNION ALL
    SELECT id AS bpm, track_id AS qty, 'play' AS origin
    FROM plays
)
SELECT origin, COUNT(*) AS n_rows, SUM(bpm) AS total_bpm
FROM combined
GROUP BY origin
ORDER BY origin
