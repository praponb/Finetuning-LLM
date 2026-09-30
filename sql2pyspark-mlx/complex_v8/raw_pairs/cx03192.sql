WITH all_events AS (
    -- Stack the branches
    SELECT agent_id AS ref_id, id AS key_id, 'ticket' AS record_type
    FROM tickets
    WHERE priority NOT IN ('P1', 'P3')
      AND opened_at >= TIMESTAMP '2025-01-13 12:00:00'
    UNION ALL
    SELECT agent_id AS ref_id, csat AS key_id, 'ticket_2' AS record_type
    FROM tickets
    WHERE channel IN ('phone', 'chat', 'web')
)
SELECT record_type, COUNT(*) AS n_rows, SUM(key_id) AS total_key_id
FROM all_events
GROUP BY record_type
ORDER BY record_type
