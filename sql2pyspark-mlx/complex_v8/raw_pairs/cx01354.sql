WITH valid_events AS (
    -- Scope the ticket_events rows
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor
    FROM ticket_events
    WHERE TO_DATE(event_at) < DATE '2025-03-14'
      AND ticket_id >= 38
),
with_details AS (
    -- Attach ticket attributes
    SELECT
        s.id,
        s.ticket_id,
        s.event_at,
        s.actor,
        t.priority AS ticket_priority,
        t.tags AS ticket_tags,
        t.opened_at AS ticket_opened_at,
        su.hired_on AS agent_hired_on
    FROM valid_events s
    LEFT JOIN tickets t ON t.id = s.ticket_id
    JOIN support_agents su ON su.id = t.agent_id
),
scored AS (
    -- Derive helper columns
    SELECT *, ARRAY_JOIN(ARRAY_SORT(ticket_tags), ',') AS ticket_tags_text
    FROM with_details
),
exploded AS (
    -- One row per ticket_tag
    SELECT
        s.id,
        s.ticket_id,
        s.event_at,
        s.actor,
        s.ticket_priority,
        s.ticket_opened_at,
        s.agent_hired_on,
        s.ticket_tags_text,
        t.ticket_tag
    FROM scored s
    LATERAL VIEW OUTER EXPLODE(s.ticket_tags) t AS ticket_tag
),
filtered AS (
    -- Rows with no matching ticket_events
    SELECT s.*
    FROM exploded s
    WHERE NOT EXISTS (
            SELECT 1
            FROM ticket_events t
            WHERE t.ticket_id = s.ticket_id
              AND t.event_type IN ('resolved', 'escalated', 'replied')
        )
)
-- Final projection
SELECT DISTINCT ticket_priority, ticket_tag
FROM filtered
ORDER BY ticket_tag DESC, ticket_priority DESC
