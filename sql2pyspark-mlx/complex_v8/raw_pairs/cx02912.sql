WITH valid_events AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor
    FROM ticket_events
    WHERE actor IN ('agent', 'system')
      AND event_at < TIMESTAMP '2025-02-12 11:00:00'
),
filtered AS (
    -- Rows with no matching ticket_events
    SELECT s.*
    FROM valid_events s
    WHERE NOT EXISTS (
            SELECT 1
            FROM ticket_events t
            WHERE t.ticket_id = s.ticket_id
              AND t.event_at >= TIMESTAMP '2025-01-31 08:00:00'
        )
),
detail_rows_events AS (
    -- One row per event
    SELECT
        s.id,
        s.ticket_id,
        s.event_type,
        t.id AS event_id,
        t.actor AS event_actor,
        t.event_at AS event_event_at
    FROM filtered s
    JOIN ticket_events t ON t.ticket_id = s.ticket_id
),
combined_details_ticket AS (
    -- Join tickets
    SELECT
        s.id,
        s.ticket_id,
        s.event_type,
        s.event_id,
        s.event_actor,
        s.event_event_at,
        t.csat AS ticket_csat,
        t.closed_at AS ticket_closed_at,
        t.agent_id AS ticket_agent_id
    FROM detail_rows_events s
    JOIN tickets t ON t.id = s.ticket_id
    WHERE t.closed_at < TIMESTAMP '2025-02-22 12:00:00'
),
profiled AS (
    -- Attach tickets metrics
    SELECT
        s.*,
        (SELECT MAX(t.opened_at) FROM tickets t WHERE t.agent_id = s.ticket_agent_id) AS last_ticket_opened_at,
        (SELECT MAX(t.opened_at) FROM tickets t WHERE t.agent_id = s.ticket_agent_id) AS last_ticket_opened_at_2
    FROM combined_details_ticket s
)
-- Subtotals with CUBE
SELECT
    ticket_agent_id,
    ticket_csat,
    MIN(ticket_csat) AS min_ticket_csat,
    ROUND(AVG(ticket_csat), 2) AS avg_ticket_csat
FROM profiled
GROUP BY CUBE(ticket_agent_id, ticket_csat)
ORDER BY ticket_agent_id NULLS LAST, ticket_csat NULLS LAST
