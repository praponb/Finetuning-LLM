WITH prepared AS (
    -- Filter ticket_events
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        CASE WHEN event_type IN ('replied', 'escalated') THEN 'group_a' WHEN event_type = 'reopened' THEN 'group_b' ELSE 'other' END AS event_type_group,
        LOWER(TRIM(actor)) AS actor_clean,
        CASE WHEN actor IN ('customer', 'system') THEN 'group_a' WHEN actor = 'agent' THEN 'group_b' ELSE 'other' END AS actor_group
    FROM ticket_events
    WHERE event_at >= TIMESTAMP '2025-01-13 14:00:00'
),
with_details_ticket AS (
    -- Enrich with dimension data
    SELECT
        s.*,
        t.tags AS ticket_tags,
        su.team AS agent_team,
        su.tier AS agent_tier
    FROM prepared s
    JOIN tickets t ON t.id = s.ticket_id
    JOIN support_agents su ON su.id = t.agent_id
    WHERE SIZE(t.tags) >= 1
),
detail_rows_events AS (
    -- One row per event
    SELECT
        s.id,
        s.ticket_id,
        s.event_at,
        s.event_type,
        s.actor,
        s.event_type_group,
        s.actor_clean,
        t.id AS event_id,
        t.actor AS event_actor,
        t.event_type AS event_event_type
    FROM with_details_ticket s
    JOIN ticket_events t ON t.ticket_id = s.ticket_id
)
-- Final projection
SELECT
    id,
    ticket_id,
    event_at,
    event_type,
    actor,
    actor_clean,
    event_id,
    event_actor,
    event_event_type,
    CONCAT(event_type_group, '#', CAST(event_id AS STRING)) AS event_type_group_ref,
    DATE_TRUNC('HOUR', event_at) AS event_at_hour
FROM detail_rows_events
ORDER BY actor_clean NULLS LAST, event_id NULLS LAST
