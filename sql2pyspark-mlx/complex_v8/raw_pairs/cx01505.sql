WITH unioned AS (
    -- Stack the branches
    SELECT id AS key_id, event_at AS happened_at
    FROM ticket_events
    WHERE actor = 'customer'
      AND HOUR(event_at) BETWEEN 8 AND 10
    UNION
    SELECT id AS key_id, event_at AS happened_at
    FROM ticket_events
    WHERE ticket_id <= 7
      AND MONTH(event_at) IN (1, 2)
)
SELECT key_id, COUNT(*) AS n_rows, MAX(happened_at) AS latest_happened_at
FROM unioned
GROUP BY key_id
ORDER BY key_id
