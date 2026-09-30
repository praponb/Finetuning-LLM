WITH combined AS (
    -- Stack the branches
    SELECT 'play' AS src, ms_played, played_at AS event_time
    FROM plays
    UNION
    SELECT 'play_2' AS src, track_id, played_at
    FROM plays
)
SELECT src, COUNT(*) AS n_rows, SUM(ms_played) AS total_ms_played, MAX(event_time) AS latest_event_time
FROM combined
GROUP BY src
ORDER BY src
