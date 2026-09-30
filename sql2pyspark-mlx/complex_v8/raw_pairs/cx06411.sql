WITH recent AS (
    -- Scope the tickets rows
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        closed_at,
        priority,
        channel,
        csat,
        tags,
        CONCAT_WS('-', priority, channel) AS priority_channel_label
    FROM tickets
    WHERE SIZE(tags) >= 2
),
detail_rows AS (
    -- One row per event
    SELECT
        s.id,
        s.requester_id,
        s.agent_id,
        s.closed_at,
        s.priority,
        s.channel,
        t.id AS event_id,
        t.event_type,
        t.actor AS event_actor
    FROM recent s
    JOIN ticket_events t ON t.ticket_id = s.id
),
labeled AS (
    -- Add calculated columns
    SELECT *, COALESCE(requester_id, 0) AS requester_id_filled
    FROM detail_rows
    WHERE channel = 'phone'
),
with_parent AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.requester_id,
        s.agent_id,
        s.priority,
        s.channel,
        s.event_id,
        s.event_type,
        s.event_actor,
        t.actor AS event_actor_2,
        t.ticket_id AS event_ticket_id,
        t.event_type AS event_event_type,
        ti.priority AS ticket_priority,
        ti.tags AS ticket_tags
    FROM labeled s
    LEFT JOIN ticket_events t ON t.id = s.event_id
    JOIN tickets ti ON ti.id = t.ticket_id
),
latest_per_key AS (
    -- Keep the latest row per event_actor, event_ticket_id
    SELECT
        id,
        requester_id,
        agent_id,
        priority,
        channel,
        event_id,
        event_type,
        event_actor,
        event_actor_2,
        event_ticket_id,
        event_event_type,
        ticket_priority,
        ticket_tags
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY event_actor, event_ticket_id ORDER BY requester_id DESC, id DESC, event_id DESC) AS row_num
        FROM with_parent
    ) ranked
    WHERE row_num = 1
),
per_group AS (
    -- Group-level totals
    SELECT
        event_actor_2,
        ARRAY_SORT(COLLECT_SET(event_actor_2)) AS event_actor_2_list,
        ARRAY_SORT(COLLECT_SET(ticket_priority)) AS ticket_priority_list,
        SUM(requester_id) FILTER (WHERE priority IS NOT NULL) AS requester_id_priority_is_not_null,
        MAX(requester_id) AS max_requester_id
    FROM latest_per_key
    GROUP BY event_actor_2
)
-- Final projection
SELECT
    event_actor_2,
    event_actor_2_list,
    ticket_priority_list,
    requester_id_priority_is_not_null,
    max_requester_id,
    SUBSTRING(TRIM(event_actor_2), 1, 3) AS event_actor_2_prefix,
    LENGTH(TRIM(event_actor_2)) AS event_actor_2_len
FROM per_group
ORDER BY event_actor_2_len
