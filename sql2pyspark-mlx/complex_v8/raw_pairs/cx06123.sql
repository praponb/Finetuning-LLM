WITH unioned AS (
    -- Stack the branches
    SELECT ticket_id, event_at, 'event' AS origin
    FROM ticket_events
    WHERE event_type <> 'escalated'
    UNION ALL
    SELECT requester_id, opened_at, 'ticket' AS origin
    FROM tickets
    WHERE opened_at >= TIMESTAMP '2025-01-25 08:00:00'
      AND csat < 4
    UNION ALL
    SELECT id, event_at, 'event_3' AS origin
    FROM ticket_events
    WHERE event_type IN ('resolved', 'reopened')
      AND event_at >= TIMESTAMP '2025-01-25 07:00:00'
)
SELECT origin, COUNT(*) AS n_rows, SUM(ticket_id) AS total_ticket_id, MAX(event_at) AS latest_event_at
FROM unioned
GROUP BY origin
ORDER BY origin
