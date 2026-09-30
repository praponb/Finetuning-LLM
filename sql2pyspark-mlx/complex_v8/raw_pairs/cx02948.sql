WITH prepared_tickets AS (
    -- Scope the tickets rows
    SELECT *
    FROM tickets
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
        t.tag_pos,
        t.tag
    FROM prepared_tickets s
    LATERAL VIEW OUTER POSEXPLODE(s.tags) t AS tag_pos, tag
),
profiled AS (
    -- Attach tickets metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FROM tickets t WHERE t.agent_id = s.agent_id) AS ticket_count,
        (SELECT COUNT(*) FILTER (WHERE t.channel IS NOT NULL) FROM tickets t WHERE t.agent_id = s.agent_id) AS ticket_channel_count,
        (SELECT MIN(t.closed_at) FROM tickets t WHERE t.agent_id = s.agent_id) AS first_ticket_closed_at
    FROM flattened s
)
SELECT
    id,
    agent_id,
    opened_at,
    closed_at,
    priority,
    channel,
    csat,
    ticket_count,
    ticket_channel_count,
    first_ticket_closed_at
FROM profiled
WHERE priority NOT IN ('P3', 'P1')
ORDER BY ticket_channel_count
