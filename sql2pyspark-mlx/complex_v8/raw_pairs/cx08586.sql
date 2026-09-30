WITH valid AS (
    -- Scope the plays rows
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        device,
        is_skipped
    FROM plays
    WHERE track_id IS NOT NULL
),
all_events AS (
    -- Stack the branches
    SELECT track_id AS entity_id, played_at AS event_time
    FROM valid
    UNION ALL
    SELECT id, played_at
    FROM plays
    WHERE device = 'car'
      AND track_id <= 19
)
SELECT entity_id, COUNT(*) AS n_rows, MAX(event_time) AS latest_event_time
FROM all_events
GROUP BY entity_id
ORDER BY entity_id
