WITH filtered AS (
    -- Scope the ticket_events rows
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        DATEDIFF(DATE '2026-01-01', event_at) AS days_since_event_at,
        CASE WHEN event_type IN ('created', 'assigned') THEN 'group_a' WHEN event_type = 'replied' THEN 'group_b' ELSE 'other' END AS event_type_group,
        DATE_FORMAT(event_at, 'MMM yyyy') AS event_at_label
    FROM ticket_events
    WHERE event_type = 'resolved'
      AND actor <> 'agent'
),
joined AS (
    -- Join tickets
    SELECT
        s.id,
        s.ticket_id,
        s.event_type,
        s.actor,
        s.days_since_event_at,
        s.event_type_group,
        t.agent_id AS ticket_agent_id
    FROM filtered s
    JOIN tickets t ON t.id = s.ticket_id
),
qualified AS (
    -- Rows that have matching ticket_events
    SELECT s.*
    FROM joined s
    WHERE EXISTS (
            SELECT 1
            FROM ticket_events t
            WHERE t.ticket_id = s.ticket_id
        )
),
enriched AS (
    -- Add calculated columns
    SELECT
        *,
        SUBSTRING(TRIM(event_type), 1, 4) AS event_type_prefix,
        SUBSTRING(TRIM(actor), 1, 2) AS actor_prefix,
        FLOOR(days_since_event_at / 100) * 100 AS days_since_event_at_bucket
    FROM qualified
),
with_parent AS (
    -- Attach ticket attributes
    SELECT
        s.id,
        s.ticket_id,
        s.actor,
        s.event_type_group,
        s.ticket_agent_id,
        s.event_type_prefix,
        s.days_since_event_at_bucket,
        t.channel AS ticket_channel
    FROM enriched s
    JOIN tickets t ON t.id = s.ticket_id
),
with_details AS (
    -- Join support_agents
    SELECT
        s.id,
        s.ticket_id,
        s.event_type_group,
        s.ticket_agent_id,
        s.event_type_prefix,
        s.ticket_channel,
        su.team AS agent_team,
        t.requester_id AS ticket_requester_id,
        t.channel AS ticket_channel_2
    FROM with_parent s
    LEFT JOIN support_agents su ON su.id = s.ticket_agent_id
    JOIN tickets t ON t.id = s.ticket_id
)
-- Final projection
SELECT
    id,
    ticket_id,
    event_type_group,
    event_type_prefix,
    ticket_channel,
    agent_team,
    ticket_requester_id,
    ticket_channel_2,
    COALESCE(ticket_channel_2, 'UNKNOWN') AS ticket_channel_2_or_unknown
FROM with_details
