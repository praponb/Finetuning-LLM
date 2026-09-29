WITH stacked AS (
    -- Stack the branches
    SELECT ticket_id, id
    FROM ticket_events
    WHERE TO_DATE(event_at) < DATE '2025-01-31'
    UNION ALL
    SELECT requester_id, agent_id
    FROM tickets
    WHERE ARRAY_CONTAINS(tags, 'upgrade')
      AND closed_at >= TIMESTAMP '2025-01-13 14:00:00'
)
SELECT ticket_id, COUNT(*) AS n_rows, SUM(id) AS total_id
FROM stacked
GROUP BY ticket_id
ORDER BY ticket_id
