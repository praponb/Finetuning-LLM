WITH all_events AS (
    -- Stack the branches
    SELECT csat, closed_at, 'ticket' AS origin
    FROM tickets
    WHERE requester_id BETWEEN 7 AND 17
      AND csat < 4
    UNION ALL
    SELECT id AS csat, event_at AS closed_at, 'event' AS origin
    FROM ticket_events
    WHERE HOUR(event_at) BETWEEN 8 AND 10
)
SELECT origin, COUNT(*) AS n_rows, SUM(csat) AS total_csat, MAX(closed_at) AS latest_closed_at
FROM all_events
GROUP BY origin
ORDER BY origin
