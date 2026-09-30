WITH joined_ranges AS (
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
     AND e.event_at <= COALESCE(t.closed_at, t.opened_at + INTERVAL 2 DAYS)
),
qualified AS (
    -- Rows with no matching ticket_events
    SELECT s.*
    FROM joined_ranges s
    WHERE NOT EXISTS (
            SELECT 1
            FROM ticket_events t
            WHERE t.ticket_id = s.ticket_id
              AND t.event_type IN ('replied', 'escalated', 'reopened')
              AND t.event_at < TIMESTAMP '2025-03-08 09:00:00'
        )
      AND s.agent_id IS NOT NULL
),
labeled AS (
    -- Label and score rows
    SELECT
        id,
        event_at,
        event_type,
        priority,
        agent_id,
        UPPER(TRIM(event_type)) AS event_type_clean,
        ROUND(hours_since_open * 1.07, 2) AS hours_since_open_adj
    FROM qualified
)
SELECT
    id,
    event_at,
    event_type,
    priority,
    agent_id,
    hours_since_open_adj,
    SUBSTRING(TRIM(priority), 1, 4) AS priority_prefix,
    SUBSTRING(TRIM(event_type_clean), 1, 2) AS event_type_clean_prefix
FROM labeled
ORDER BY hours_since_open_adj DESC
