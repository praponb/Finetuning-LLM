WITH filtered AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        DATEDIFF(DATE '2026-01-01', event_at) AS days_since_event_at,
        LENGTH(TRIM(event_type)) AS event_type_len
    FROM ticket_events
    WHERE event_at < TIMESTAMP '2025-02-12 09:00:00'
      AND ticket_id IS NOT NULL
),
with_details AS (
    -- Join tickets
    SELECT
        s.id,
        s.ticket_id,
        s.event_type,
        s.actor,
        s.days_since_event_at,
        s.event_type_len,
        t.agent_id AS ticket_agent_id,
        t.opened_at AS ticket_opened_at,
        t.tags AS ticket_tags,
        su.tier AS agent_tier,
        su.agent_name
    FROM filtered s
    JOIN tickets t ON t.id = s.ticket_id
    LEFT JOIN support_agents su ON su.id = t.agent_id
)
-- Native SQL UNPIVOT (wide to long)
SELECT id, field, value
FROM (
    SELECT id, event_type_len, days_since_event_at
    FROM with_details
) src
UNPIVOT (
    value FOR field IN (event_type_len, days_since_event_at)
) unp
