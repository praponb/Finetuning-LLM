WITH combined AS (
    -- Stack the branches
    SELECT ms_played, played_at AS event_time, listener_id AS qty
    FROM plays
    WHERE ms_played >= 62294
      AND listener_id IS NOT NULL
    UNION ALL
    SELECT track_id, played_at, ms_played
    FROM plays
    UNION ALL
    SELECT ms_played, played_at, id
    FROM plays
    WHERE track_id >= 25
      AND listener_id IS NOT NULL
)
SELECT ms_played, COUNT(*) AS n_rows, SUM(qty) AS total_qty, MAX(event_time) AS latest_event_time
FROM combined
GROUP BY ms_played
ORDER BY ms_played
