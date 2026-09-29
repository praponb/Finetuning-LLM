WITH base AS (
    -- Filter tickets
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
        ROUND((UNIX_TIMESTAMP(closed_at) - UNIX_TIMESTAMP(opened_at)) / 3600, 1) AS hours_opened_at_to_closed_at,
        UPPER(TRIM(priority)) AS priority_clean
    FROM tickets
    WHERE NOT ARRAY_CONTAINS(tags, 'bug')
      AND agent_id <= 8
),
with_stats AS (
    -- Attach ticket_events metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FILTER (WHERE t.event_type IS NOT NULL) FROM ticket_events t WHERE t.ticket_id = s.id) AS event_event_type_count,
        (SELECT MAX(t.event_at) FROM ticket_events t WHERE t.ticket_id = s.id) AS last_event_at
    FROM base s
),
sequenced AS (
    -- Window metrics over several partitions
    SELECT
        id,
        requester_id,
        closed_at,
        priority,
        csat,
        tags,
        hours_opened_at_to_closed_at,
        priority_clean,
        event_event_type_count,
        LAST_VALUE(closed_at) OVER (PARTITION BY csat ORDER BY requester_id, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_closed_at,
        SIZE(COLLECT_SET(priority) OVER w_agent_id_event_event_type_co) AS distinct_priority_in_agent_id_event_event_type_c
    FROM with_stats
    WINDOW w_agent_id_event_event_type_co AS (PARTITION BY agent_id, event_event_type_count)
),
with_children_events AS (
    -- One row per event
    SELECT
        s.id,
        s.requester_id,
        s.priority,
        s.csat,
        s.hours_opened_at_to_closed_at,
        s.last_closed_at,
        s.distinct_priority_in_agent_id_event_event_type_c,
        t.id AS event_id,
        t.event_type,
        t.event_at
    FROM sequenced s
    JOIN ticket_events t ON t.ticket_id = s.id AND t.event_type NOT IN ('replied', 'created')
),
detail_rows_events AS (
    -- One row per event
    SELECT
        s.id,
        s.requester_id,
        s.priority,
        s.csat,
        s.hours_opened_at_to_closed_at,
        s.distinct_priority_in_agent_id_event_event_type_c,
        s.event_id,
        s.event_type,
        s.event_at,
        t.id AS event_id_2,
        t.actor AS event_actor,
        t.event_type AS event_event_type
    FROM with_children_events s
    JOIN ticket_events t ON t.ticket_id = s.id
)
SELECT
    id,
    requester_id,
    priority,
    csat,
    hours_opened_at_to_closed_at,
    event_type,
    event_id_2,
    event_event_type
FROM detail_rows_events
WHERE priority = 'P4'
ORDER BY hours_opened_at_to_closed_at DESC, event_type DESC NULLS FIRST
