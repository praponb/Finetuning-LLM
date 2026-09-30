WITH all_events AS (
    -- Stack the branches
    SELECT ms_played AS entity_id, played_at, 'play' AS src
    FROM plays
    WHERE played_at < TIMESTAMP '2025-03-08 10:00:00'
    UNION
    SELECT ms_played, played_at, 'play_2' AS src
    FROM plays
    WHERE device = 'car'
)
SELECT src, COUNT(*) AS n_rows, SUM(entity_id) AS total_entity_id, MAX(played_at) AS latest_played_at
FROM all_events
GROUP BY src
ORDER BY src
