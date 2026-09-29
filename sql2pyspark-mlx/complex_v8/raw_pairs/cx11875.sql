WITH base AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor
    FROM ticket_events
),
with_activity AS (
    -- Rows that have matching ticket_events
    SELECT s.*
    FROM base s
    WHERE s.ticket_id IN (
            SELECT t.ticket_id
            FROM ticket_events t
            WHERE t.event_at >= TIMESTAMP '2025-01-25 10:00:00'
              AND t.event_type NOT IN ('replied', 'created')
        )
),
tagged AS (
    -- Label and score rows
    SELECT *, CONCAT(event_type, '#', CAST(id AS STRING)) AS event_type_ref
    FROM with_activity
),
with_details AS (
    -- Join tickets
    SELECT
        s.id,
        s.ticket_id,
        s.event_at,
        s.event_type,
        s.actor,
        s.event_type_ref,
        t.agent_id AS ticket_agent_id,
        t.csat AS ticket_csat,
        t.tags AS ticket_tags
    FROM tagged s
    LEFT JOIN tickets t ON t.id = s.ticket_id
)
SELECT *
FROM (
    SELECT ticket_agent_id, event_type, actor, ticket_csat
    FROM with_details
) src
PIVOT (
    MAX(ticket_csat)
    FOR actor IN ('customer', 'system')
)
