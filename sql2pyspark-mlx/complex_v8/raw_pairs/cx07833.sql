WITH matched AS (
    -- Events that happened while the ticket was open
    SELECT
        e.id,
        e.ticket_id,
        e.event_at,
        e.event_type,
        t.priority,
        t.agent_id,
        ROUND((UNIX_TIMESTAMP(e.event_at) - UNIX_TIMESTAMP(t.opened_at)) / 3600, 2) AS hours_since_open
    FROM ticket_events e
    JOIN tickets t
      ON t.id = e.ticket_id
     AND e.event_at >= t.opened_at
     AND e.event_at <= COALESCE(t.closed_at, t.opened_at + INTERVAL 3 DAYS)
),
labeled AS (
    -- Add calculated columns
    SELECT *, CONCAT(event_type, '#', CAST(id AS STRING)) AS event_type_ref
    FROM matched
    WHERE MONTH(event_at) IN (1, 2)
),
filtered AS (
    -- Rows with no matching ticket_events
    SELECT s.*
    FROM labeled s
    WHERE NOT EXISTS (
            SELECT 1
            FROM ticket_events t
            WHERE t.ticket_id = s.ticket_id
              AND TO_DATE(t.event_at) < DATE '2025-03-02'
              AND t.event_type IS NOT NULL
        )
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT ticket_id, priority, hours_since_open
    FROM filtered
) src
PIVOT (
    MIN(hours_since_open)
    FOR priority IN ('P2', 'P4')
)
