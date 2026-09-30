WITH combined AS (
    -- Stack the branches
    SELECT listener_id AS key_id, played_at AS happened_at
    FROM plays
    WHERE track_id >= 12
    UNION ALL
    SELECT id, played_at
    FROM plays
    WHERE device = 'tv'
      AND MONTH(played_at) IN (1, 2)
)
SELECT key_id, COUNT(*) AS n_rows, MAX(happened_at) AS latest_happened_at
FROM combined
GROUP BY key_id
ORDER BY key_id
