WITH valid AS (
    -- Scope the tickets rows
    SELECT
        id,
        requester_id,
        agent_id,
        closed_at,
        priority,
        tags,
        YEAR(closed_at) AS closed_at_year,
        LENGTH(TRIM(channel)) AS channel_len,
        COALESCE(priority, 'UNKNOWN') AS priority_or_unknown
    FROM tickets
    WHERE closed_at < TIMESTAMP '2025-03-04 05:00:00'
      AND SIZE(tags) >= 2
),
ticket_stats AS (
    -- Per-agent tickets metrics
    SELECT
        agent_id,
        COUNT(*) AS ticket_count,
        SUM(requester_id) AS total_ticket_requester_id
    FROM tickets
    GROUP BY agent_id
),
with_stats AS (
    -- Attach tickets metrics
    SELECT
        s.*,
        COALESCE(t.ticket_count, 0) AS ticket_count,
        t.total_ticket_requester_id
    FROM valid s
    LEFT JOIN ticket_stats t ON t.agent_id = s.agent_id
),
with_parent AS (
    -- Attach agent attributes
    SELECT
        s.id,
        s.requester_id,
        s.agent_id,
        s.closed_at,
        s.priority,
        s.tags,
        s.closed_at_year,
        s.channel_len,
        s.priority_or_unknown,
        s.ticket_count,
        s.total_ticket_requester_id,
        su.agent_name
    FROM with_stats s
    JOIN support_agents su ON su.id = s.agent_id
),
without_matches AS (
    -- Compare against the overall avg
    SELECT *
    FROM with_parent
    WHERE requester_id > (SELECT AVG(requester_id) FROM with_parent)
)
-- Subtotals with GROUPING SETS
SELECT
    channel_len,
    priority,
    ticket_count,
    SUM(total_ticket_requester_id) AS total_total_ticket_requester_id,
    SUM(requester_id) AS total_requester_id
FROM without_matches
GROUP BY GROUPING SETS ((channel_len, priority, ticket_count), (channel_len), (priority))
ORDER BY channel_len NULLS LAST, priority NULLS LAST, ticket_count NULLS LAST
