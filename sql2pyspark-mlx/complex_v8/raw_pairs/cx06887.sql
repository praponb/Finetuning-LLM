WITH scoped_tickets AS (
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
        closed_at + INTERVAL 7 DAYS AS closed_at_plus_7d
    FROM tickets
    WHERE channel IS NOT NULL
),
labeled AS (
    -- Add calculated columns
    SELECT *, COALESCE(priority, 'UNKNOWN') AS priority_or_unknown
    FROM scoped_tickets
    WHERE closed_at >= TIMESTAMP '2025-01-14 08:00:00'
),
exploded_detail AS (
    -- One row per event
    SELECT
        s.id,
        s.agent_id,
        s.channel,
        s.csat,
        s.priority_or_unknown,
        t.id AS event_id,
        t.actor AS event_actor,
        t.event_at,
        t.event_type
    FROM labeled s
    JOIN ticket_events t ON t.ticket_id = s.id
),
latest AS (
    -- Keep the latest row per agent_id, event_actor
    SELECT
        id,
        agent_id,
        channel,
        csat,
        priority_or_unknown,
        event_id,
        event_actor,
        event_at,
        event_type
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY agent_id, event_actor ORDER BY event_at DESC, id DESC, event_id DESC) AS recency_rank
        FROM exploded_detail
    ) ranked
    WHERE recency_rank = 1
)
-- Final projection
SELECT
    id,
    agent_id,
    event_id,
    event_actor,
    event_type,
    DATE_FORMAT(event_at, 'yyyy-MM-dd HH:00') AS event_at_label
FROM latest
ORDER BY event_id DESC, id
LIMIT 10
