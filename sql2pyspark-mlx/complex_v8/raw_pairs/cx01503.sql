WITH prepared_events AS (
    -- Filter ticket_events
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        COALESCE(actor, 'UNKNOWN') AS actor_or_unknown,
        CONCAT(event_type, '#', CAST(ticket_id AS STRING)) AS event_type_ref,
        SUBSTRING(TRIM(event_type), 1, 3) AS event_type_prefix
    FROM ticket_events
    WHERE ticket_id >= 43
),
annotated AS (
    -- Derive helper columns
    SELECT
        *,
        LOWER(REGEXP_REPLACE(TRIM(event_type_ref), ' ', '_')) AS event_type_ref_slug,
        CASE WHEN actor_or_unknown IN ('system', 'UNKNOWN') THEN 'group_a' WHEN actor_or_unknown = 'agent' THEN 'group_b' ELSE 'other' END AS actor_or_unknown_group
    FROM prepared_events
),
enriched_ticket AS (
    -- Join tickets
    SELECT
        s.id,
        s.ticket_id,
        s.event_at,
        s.event_type,
        s.actor_or_unknown,
        s.event_type_prefix,
        s.event_type_ref_slug,
        s.actor_or_unknown_group,
        t.priority AS ticket_priority,
        t.closed_at AS ticket_closed_at,
        t.channel AS ticket_channel
    FROM annotated s
    JOIN tickets t ON t.id = s.ticket_id
),
joined AS (
    -- Attach ticket attributes
    SELECT
        s.id,
        s.ticket_id,
        s.event_at,
        s.event_type,
        s.actor_or_unknown,
        s.event_type_prefix,
        s.event_type_ref_slug,
        s.ticket_priority,
        t.priority AS ticket_priority_2
    FROM enriched_ticket s
    JOIN tickets t ON t.id = s.ticket_id AND t.csat <= 5
),
joined_ticket AS (
    -- Attach ticket attributes
    SELECT
        s.id,
        s.ticket_id,
        s.event_type,
        s.actor_or_unknown,
        s.event_type_prefix,
        s.event_type_ref_slug,
        s.ticket_priority,
        t.requester_id AS ticket_requester_id,
        su.tier AS agent_tier,
        su.agent_name
    FROM joined s
    LEFT JOIN tickets t ON t.id = s.ticket_id AND HOUR(t.closed_at) BETWEEN 7 AND 9
    JOIN support_agents su ON su.id = t.agent_id
),
qualified AS (
    -- Rows with no matching ticket_events
    SELECT s.*
    FROM joined_ticket s
    WHERE NOT EXISTS (
            SELECT 1
            FROM ticket_events t
            WHERE t.ticket_id = s.ticket_id
              AND t.actor = 'system'
              AND t.event_type IN ('created', 'replied', 'resolved')
        )
      AND s.actor_or_unknown IN ('system', 'UNKNOWN')
)
-- Final projection
SELECT
    id,
    ticket_id,
    event_type,
    event_type_prefix,
    event_type_ref_slug,
    ticket_priority,
    agent_tier,
    agent_name,
    CASE WHEN ticket_requester_id >= 20 THEN 'A' WHEN ticket_requester_id >= 9 THEN 'B' ELSE 'C' END AS ticket_requester_id_tier
FROM qualified
WHERE ticket_priority = 'P4'
ORDER BY id NULLS LAST, ticket_id DESC
