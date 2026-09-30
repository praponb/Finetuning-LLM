WITH prepared AS (
    -- Filter tickets
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        closed_at,
        channel,
        csat,
        tags,
        DATE_FORMAT(opened_at, 'yyyy-MM-dd') AS opened_at_label
    FROM tickets
    WHERE agent_id <= 4
),
exploded_detail_events AS (
    -- One row per event
    SELECT
        s.id,
        s.requester_id,
        s.agent_id,
        s.opened_at,
        s.closed_at,
        s.channel,
        s.tags,
        s.opened_at_label,
        t.id AS event_id,
        t.actor AS event_actor,
        t.event_type,
        t.event_at
    FROM prepared s
    LEFT JOIN ticket_events t ON t.ticket_id = s.id AND t.event_at >= TIMESTAMP '2025-01-25 11:00:00'
),
tagged AS (
    -- Label and score rows
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        closed_at,
        channel,
        tags,
        opened_at_label,
        event_id,
        event_actor,
        event_type,
        COALESCE(requester_id, 0) AS requester_id_filled,
        ARRAY_JOIN(ARRAY_SORT(tags), ',') AS tags_text
    FROM exploded_detail_events
),
qualified AS (
    -- Rows that have matching tickets
    SELECT s.*
    FROM tagged s
    WHERE EXISTS (
            SELECT 1
            FROM tickets t
            WHERE t.agent_id = s.agent_id
              AND t.closed_at < TIMESTAMP '2025-03-09 04:00:00'
              AND t.priority NOT IN ('P4', 'P1')
        )
      AND s.closed_at < TIMESTAMP '2025-02-10 04:00:00'
)
SELECT id, event_id, field, metric_value
FROM (
    SELECT id, event_id, requester_id_filled, requester_id
    FROM qualified
) src
UNPIVOT INCLUDE NULLS (
    metric_value FOR field IN (requester_id_filled, requester_id)
) unp
ORDER BY id, event_id, field
