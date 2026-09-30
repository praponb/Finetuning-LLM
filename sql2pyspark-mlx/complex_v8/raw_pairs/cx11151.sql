WITH prepared AS (
    -- Scope the ticket_events rows
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor
    FROM ticket_events
),
enriched_ticket AS (
    -- Join tickets
    SELECT s.*, t.tags AS ticket_tags, t.opened_at AS ticket_opened_at
    FROM prepared s
    LEFT JOIN tickets t ON t.id = s.ticket_id
),
labeled AS (
    -- Derive helper columns
    SELECT
        id,
        ticket_id,
        event_type,
        actor,
        ticket_opened_at,
        ticket_opened_at + INTERVAL 15 MINUTES AS ticket_opened_at_plus_15m,
        LOWER(TRIM(actor)) AS actor_clean
    FROM enriched_ticket
    WHERE event_at < TIMESTAMP '2025-02-12 13:00:00'
),
annotated AS (
    -- Label and score rows
    SELECT
        *,
        LENGTH(TRIM(actor_clean)) AS actor_clean_len,
        CASE WHEN event_type IN ('resolved', 'assigned') THEN 'group_a' WHEN event_type = 'reopened' THEN 'group_b' ELSE 'other' END AS event_type_group
    FROM labeled
),
enriched_ticket_2 AS (
    -- Join tickets
    SELECT s.*, t.tags AS ticket_tags, su.agent_name
    FROM annotated s
    JOIN tickets t ON t.id = s.ticket_id
    LEFT JOIN support_agents su ON su.id = t.agent_id
)
SELECT
    id,
    ticket_id,
    event_type,
    ticket_opened_at,
    ticket_opened_at_plus_15m,
    actor_clean,
    actor_clean_len,
    event_type_group,
    ticket_tags,
    agent_name,
    ticket_opened_at + INTERVAL 15 MINUTES AS ticket_opened_at_plus_15m_2,
    TO_DATE(ticket_opened_at) AS ticket_opened_at_date
FROM enriched_ticket_2
ORDER BY ticket_opened_at_date NULLS LAST, ticket_id DESC
