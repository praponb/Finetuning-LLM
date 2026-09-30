WITH prepared AS (
    -- Filter tickets
    SELECT
        id,
        requester_id,
        agent_id,
        priority,
        tags
    FROM tickets
    WHERE agent_id IS NOT NULL
),
profiled AS (
    -- Attach tickets metrics
    SELECT
        s.*,
        (SELECT MAX(t.closed_at) FROM tickets t WHERE t.agent_id = s.agent_id) AS last_ticket_closed_at,
        (SELECT COUNT(*) FROM tickets t WHERE t.agent_id = s.agent_id) AS ticket_count
    FROM prepared s
),
flattened AS (
    -- One row per tag
    SELECT
        s.id,
        s.requester_id,
        s.agent_id,
        s.priority,
        s.last_ticket_closed_at,
        s.ticket_count,
        t.tag
    FROM profiled s
    LATERAL VIEW OUTER EXPLODE(s.tags) t AS tag
),
scored AS (
    -- Add calculated columns
    SELECT
        id,
        agent_id,
        ticket_count,
        tag,
        DAYOFWEEK(last_ticket_closed_at) AS last_ticket_closed_at_dayofweek,
        COALESCE(priority, 'UNKNOWN') AS priority_or_unknown,
        DATE_TRUNC('YEAR', last_ticket_closed_at) AS last_ticket_closed_at_year
    FROM flattened
    WHERE agent_id <= 8
)
-- Final projection
SELECT
    id,
    agent_id,
    ticket_count,
    tag,
    last_ticket_closed_at_dayofweek,
    priority_or_unknown,
    last_ticket_closed_at_year,
    COALESCE(ticket_count, 0) AS ticket_count_filled
FROM scored
ORDER BY ticket_count_filled NULLS LAST, ticket_count DESC NULLS FIRST
