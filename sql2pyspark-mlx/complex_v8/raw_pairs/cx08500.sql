WITH scoped_tickets AS (
    -- Filter tickets
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        priority,
        tags,
        GREATEST(requester_id, csat) AS greatest_requester_id_csat
    FROM tickets
    WHERE (SIZE(tags) >= 1 OR agent_id <= 5)
),
exploded AS (
    -- One row per tag
    SELECT
        s.id,
        s.requester_id,
        s.agent_id,
        s.opened_at,
        s.priority,
        s.greatest_requester_id_csat,
        t.tag_pos,
        t.tag
    FROM scoped_tickets s
    LATERAL VIEW POSEXPLODE(s.tags) t AS tag_pos, tag
),
with_parent AS (
    -- Attach agent attributes
    SELECT
        s.id,
        s.requester_id,
        s.agent_id,
        s.opened_at,
        s.priority,
        s.greatest_requester_id_csat,
        s.tag_pos,
        s.tag,
        su.hired_on AS agent_hired_on,
        su.agent_name
    FROM exploded s
    JOIN support_agents su ON su.id = s.agent_id
),
with_stats AS (
    -- Attach ticket_events metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FILTER (WHERE t.event_type IN ('replied', 'assigned')) FROM ticket_events t WHERE t.ticket_id = s.id) AS event_event_type_count,
        (SELECT COUNT(*) FROM ticket_events t WHERE t.ticket_id = s.id) AS event_count
    FROM with_parent s
),
agent_id_stats AS (
    -- Max tag_pos per agent_id
    SELECT agent_id, MAX(tag_pos) AS max_tag_pos
    FROM with_stats
    GROUP BY agent_id
),
above_average AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.max_tag_pos,
        ROUND(s.tag_pos - g.max_tag_pos, 2) AS tag_pos_minus_max
    FROM with_stats s
    JOIN agent_id_stats g ON g.agent_id = s.agent_id
    WHERE s.tag_pos = g.max_tag_pos
),
with_stats_2 AS (
    -- Attach tickets metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FROM tickets t WHERE t.agent_id = s.agent_id) AS ticket_count,
        (SELECT MAX(t.closed_at) FROM tickets t WHERE t.agent_id = s.agent_id) AS last_ticket_closed_at
    FROM above_average s
),
with_trends AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        DENSE_RANK() OVER w_priority_ticket_count_by_age AS agent_hired_on_dense_rank,
        ROUND(PERCENT_RANK() OVER (PARTITION BY priority, ticket_count ORDER BY agent_hired_on DESC NULLS FIRST), 3) AS agent_hired_on_percent_rank,
        ROUND(AVG(tag_pos) OVER (PARTITION BY agent_id ORDER BY opened_at, id, tag_pos ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS tag_pos_avg_7,
        RANK() OVER (PARTITION BY priority, ticket_count ORDER BY agent_hired_on DESC NULLS FIRST) AS agent_hired_on_rank
    FROM with_stats_2
    WINDOW w_priority_ticket_count_by_age AS (PARTITION BY priority, ticket_count ORDER BY agent_hired_on DESC NULLS FIRST, id, tag_pos)
)
SELECT
    id,
    requester_id,
    agent_id,
    opened_at,
    priority,
    greatest_requester_id_csat,
    tag_pos,
    tag,
    agent_name,
    event_event_type_count,
    event_count,
    max_tag_pos,
    tag_pos_minus_max,
    ticket_count,
    agent_hired_on_dense_rank,
    agent_hired_on_percent_rank,
    tag_pos_avg_7,
    agent_hired_on_rank,
    ROUND((UNIX_TIMESTAMP(last_ticket_closed_at) - UNIX_TIMESTAMP(opened_at)) / 3600, 1) AS hours_opened_at_to_last_ticket_closed_at,
    DATE_TRUNC('MONTH', opened_at) AS opened_at_month
FROM with_trends
ORDER BY opened_at DESC
