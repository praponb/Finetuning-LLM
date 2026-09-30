WITH valid AS (
    -- Filter ticket_events
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        DATE_TRUNC('MONTH', event_at) AS event_at_month
    FROM ticket_events
    WHERE event_type IS NOT NULL
),
with_children_events AS (
    -- One row per event
    SELECT
        s.id,
        s.ticket_id,
        s.event_at,
        s.event_type,
        s.actor,
        s.event_at_month,
        t.id AS event_id,
        t.event_at AS event_event_at,
        t.event_type AS event_event_type,
        t.actor AS event_actor
    FROM valid s
    LEFT JOIN ticket_events t ON t.ticket_id = s.ticket_id AND t.event_at < TIMESTAMP '2025-02-12 09:00:00'
),
joined AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.ticket_id,
        s.event_at,
        s.event_type,
        s.actor,
        s.event_at_month,
        s.event_id,
        s.event_event_type,
        s.event_actor,
        t.tags AS ticket_tags,
        t.channel AS ticket_channel,
        t.agent_id AS ticket_agent_id
    FROM with_children_events s
    JOIN tickets t ON t.id = s.ticket_id
),
summarized_rows AS (
    -- Attach ticket_events metrics
    SELECT
        s.*,
        (SELECT MAX(t.event_at) FROM ticket_events t WHERE t.ticket_id = s.ticket_id) AS last_event_at
    FROM joined s
),
line_level AS (
    -- One row per event
    SELECT
        s.id,
        s.ticket_id,
        s.event_at,
        s.event_type,
        s.actor,
        s.event_id,
        s.event_event_type,
        s.ticket_tags,
        s.ticket_channel,
        s.ticket_agent_id,
        s.last_event_at,
        t.id AS event_id_2,
        t.actor AS event_actor_2,
        t.event_type AS event_event_type_2,
        t.event_at AS event_event_at
    FROM summarized_rows s
    JOIN ticket_events t ON t.ticket_id = s.ticket_id
),
tagged AS (
    -- Derive helper columns
    SELECT
        id,
        event_at,
        event_type,
        event_id,
        event_event_type,
        ticket_tags,
        ticket_channel,
        ticket_agent_id,
        event_id_2,
        event_actor_2,
        event_event_at,
        CASE WHEN event_actor_2 IN ('agent', 'system') THEN 'group_a' WHEN event_actor_2 = 'customer' THEN 'group_b' ELSE 'other' END AS event_actor_2_group,
        CONCAT(event_event_type_2, '#', CAST(ticket_id AS STRING)) AS event_event_type_2_ref
    FROM line_level
    WHERE event_event_type = 'created'
)
SELECT
    id,
    event_at,
    event_type,
    event_event_type,
    ticket_tags,
    ticket_channel,
    ticket_agent_id,
    event_id_2,
    event_actor_2,
    event_event_type_2_ref,
    CONCAT_WS(': ', event_actor_2_group, event_actor_2) AS event_actor_2_group_event_actor_2_label
FROM tagged
ORDER BY id, event_actor_2 NULLS LAST
