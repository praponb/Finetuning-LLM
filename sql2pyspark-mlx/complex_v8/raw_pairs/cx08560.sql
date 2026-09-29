WITH filtered AS (
    -- Filter ticket_events
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        COALESCE(actor, 'UNKNOWN') AS actor_or_unknown,
        LENGTH(TRIM(event_type)) AS event_type_len
    FROM ticket_events
    WHERE MONTH(event_at) IN (1, 3)
),
combined_details_ticket AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.ticket_id,
        s.event_at,
        s.event_type,
        s.actor,
        s.actor_or_unknown,
        s.event_type_len,
        t.tags AS ticket_tags,
        su.agent_name
    FROM filtered s
    LEFT JOIN tickets t ON t.id = s.ticket_id AND TO_DATE(t.opened_at) < DATE '2025-02-18'
    JOIN support_agents su ON su.id = t.agent_id
),
derived AS (
    -- Add calculated columns
    SELECT
        id,
        ticket_id,
        event_type,
        actor,
        actor_or_unknown,
        agent_name,
        SUBSTRING(TRIM(agent_name), 1, 4) AS agent_name_prefix
    FROM combined_details_ticket
),
scored AS (
    -- Add calculated columns
    SELECT
        id,
        ticket_id,
        event_type,
        actor,
        actor_or_unknown,
        agent_name,
        CONCAT(actor_or_unknown, '#', CAST(ticket_id AS STRING)) AS actor_or_unknown_ref,
        LOWER(REGEXP_REPLACE(TRIM(agent_name_prefix), ' ', '_')) AS agent_name_prefix_slug
    FROM derived
),
tagged AS (
    -- Add calculated columns
    SELECT
        *,
        CASE WHEN actor IN ('agent', 'customer') THEN 'group_a' WHEN actor = 'system' THEN 'group_b' ELSE 'other' END AS actor_group,
        COALESCE(actor_or_unknown, 'UNKNOWN') AS actor_or_unknown_or_unknown
    FROM scored
),
qualified AS (
    -- Keep rows whose ticket matches
    SELECT s.*
    FROM tagged s
    WHERE NOT EXISTS (
            SELECT 1
            FROM tickets t
            WHERE t.id = s.ticket_id
              AND t.channel = 'email'
        )
),
with_parent AS (
    -- Attach ticket attributes
    SELECT
        s.id,
        s.ticket_id,
        s.event_type,
        s.actor,
        s.actor_or_unknown,
        s.agent_name,
        s.agent_name_prefix_slug,
        s.actor_group,
        s.actor_or_unknown_or_unknown,
        t.tags AS ticket_tags,
        t.csat AS ticket_csat,
        t.opened_at AS ticket_opened_at
    FROM qualified s
    LEFT JOIN tickets t ON t.id = s.ticket_id
)
-- Final projection
SELECT
    id,
    actor,
    actor_or_unknown,
    agent_name_prefix_slug,
    actor_group,
    actor_or_unknown_or_unknown,
    ticket_tags,
    ticket_csat,
    ticket_opened_at,
    SIZE(ticket_tags) AS ticket_tags_count
FROM with_parent
WHERE LENGTH(TRIM(agent_name)) >= 6
ORDER BY id DESC NULLS FIRST, actor_or_unknown_or_unknown
