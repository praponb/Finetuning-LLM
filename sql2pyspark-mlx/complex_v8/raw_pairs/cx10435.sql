WITH filtered AS (
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
        ROUND((UNIX_TIMESTAMP(opened_at) - UNIX_TIMESTAMP(closed_at)) / 3600, 1) AS hours_closed_at_to_opened_at,
        CASE WHEN requester_id >= 22 THEN 'large' WHEN requester_id >= 7 THEN 'medium' ELSE 'small' END AS requester_id_tier,
        DATE_TRUNC('DAY', closed_at) AS closed_at_day
    FROM tickets
    WHERE priority <> 'P2'
      AND channel = 'chat'
),
per_label AS (
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
        s.closed_at_day,
        t.tag
    FROM filtered s
    LATERAL VIEW EXPLODE(s.tags) t AS tag
),
line_level AS (
    -- One row per event
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
        s.tag,
        t.id AS event_id,
        t.event_at,
        t.event_type
    FROM per_label s
    LEFT JOIN ticket_events t ON t.ticket_id = s.id
),
combined_details AS (
    -- Join support_agents
    SELECT
        s.id,
        s.agent_id,
        s.opened_at,
        s.priority,
        s.channel,
        s.csat,
        s.hours_closed_at_to_opened_at,
        s.requester_id_tier,
        s.tag,
        s.event_id,
        s.event_at,
        s.event_type,
        su.hired_on AS agent_hired_on,
        t.event_at AS event_event_at
    FROM line_level s
    JOIN support_agents su ON su.id = s.agent_id
    LEFT JOIN ticket_events t ON t.id = s.event_id
    WHERE su.hired_on IS NOT NULL
),
qualified AS (
    -- Rows that have matching tickets
    SELECT s.*
    FROM combined_details s
    WHERE EXISTS (
            SELECT 1
            FROM tickets t
            WHERE t.agent_id = s.agent_id
              AND t.channel IN ('chat', 'phone')
        )
)
-- Final projection
SELECT
    id,
    agent_id,
    opened_at,
    priority,
    channel,
    tag,
    event_id,
    event_at,
    event_type,
    agent_hired_on
FROM qualified
ORDER BY tag, id DESC, event_id
LIMIT 10
