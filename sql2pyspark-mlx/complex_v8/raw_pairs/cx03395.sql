WITH prepared AS (
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
        ROUND((UNIX_TIMESTAMP(opened_at) - UNIX_TIMESTAMP(closed_at)) / 3600, 1) AS hours_closed_at_to_opened_at,
        CASE WHEN requester_id >= 17 THEN 'large' WHEN requester_id >= 5 THEN 'medium' ELSE 'small' END AS requester_id_tier
    FROM tickets
),
latest AS (
    -- Keep the earliest row per agent_id, priority
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
        hours_closed_at_to_opened_at,
        requester_id_tier
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY agent_id, priority ORDER BY requester_id, id) AS row_num
        FROM prepared
    ) ranked
    WHERE row_num = 1
),
flattened AS (
    -- One row per tag
    SELECT
        s.id,
        s.requester_id,
        s.agent_id,
        s.opened_at,
        s.closed_at,
        s.priority,
        s.channel,
        s.csat,
        s.hours_closed_at_to_opened_at,
        s.requester_id_tier,
        t.tag
    FROM latest s
    LATERAL VIEW EXPLODE(s.tags) t AS tag
),
screened AS (
    -- Rows with no matching ticket_events
    SELECT s.*
    FROM flattened s
    WHERE NOT EXISTS (
            SELECT 1
            FROM ticket_events t
            WHERE t.ticket_id = s.id
              AND t.event_type = 'assigned'
        )
      AND s.agent_id IS NOT NULL
),
detail_rows AS (
    -- One row per event
    SELECT
        s.id,
        s.requester_id,
        s.agent_id,
        s.closed_at,
        s.priority,
        s.requester_id_tier,
        s.tag,
        t.id AS event_id,
        t.event_type,
        t.actor AS event_actor,
        t.event_at
    FROM screened s
    JOIN ticket_events t ON t.ticket_id = s.id
)
-- Final projection
SELECT
    id,
    agent_id,
    closed_at,
    priority,
    requester_id_tier,
    tag,
    event_id,
    event_type,
    event_actor,
    DATE_FORMAT(event_at, 'yyyy-MM-dd') AS event_at_label
FROM detail_rows
ORDER BY tag DESC, id, event_id
LIMIT 20
