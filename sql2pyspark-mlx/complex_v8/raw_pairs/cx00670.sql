WITH prepared AS (
    -- Scope the tickets rows
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        closed_at,
        channel,
        tags,
        CASE WHEN requester_id >= 23 THEN 'HIGH' WHEN requester_id >= 7 THEN 'MEDIUM' ELSE 'LOW' END AS requester_id_tier,
        HOUR(closed_at) AS closed_at_hour,
        ROUND((UNIX_TIMESTAMP(closed_at) - UNIX_TIMESTAMP(opened_at)) / 3600, 1) AS hours_opened_at_to_closed_at
    FROM tickets
    WHERE agent_id >= 5
),
with_activity AS (
    -- Rows that have matching tickets
    SELECT s.*
    FROM prepared s
    WHERE EXISTS (
            SELECT 1
            FROM tickets t
            WHERE t.agent_id = s.agent_id
        )
      AND s.hours_opened_at_to_closed_at >= -1200.0
),
ticket_stats AS (
    -- Per-agent tickets metrics
    SELECT
        agent_id,
        MAX(closed_at) AS last_ticket_closed_at,
        COUNT(*) FILTER (WHERE priority NOT IN ('P1', 'P2')) AS ticket_priority_count,
        COUNT(*) FILTER (WHERE channel = 'phone') AS ticket_channel_count
    FROM tickets
    GROUP BY agent_id
),
profiled AS (
    -- Attach tickets metrics
    SELECT
        s.*,
        t.last_ticket_closed_at,
        COALESCE(t.ticket_priority_count, 0) AS ticket_priority_count,
        COALESCE(t.ticket_channel_count, 0) AS ticket_channel_count
    FROM with_activity s
    LEFT JOIN ticket_stats t ON t.agent_id = s.agent_id
),
agent_id_stats AS (
    -- Avg ticket_priority_count per agent_id
    SELECT agent_id, AVG(ticket_priority_count) AS avg_ticket_priority_count
    FROM profiled
    GROUP BY agent_id
),
vs_group AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_ticket_priority_count,
        ROUND(s.ticket_priority_count - g.avg_ticket_priority_count, 2) AS ticket_priority_count_minus_avg
    FROM profiled s
    JOIN agent_id_stats g ON g.agent_id = s.agent_id
    WHERE s.ticket_priority_count > g.avg_ticket_priority_count
),
with_parent_agent AS (
    -- Attach agent attributes
    SELECT s.*, su.agent_name
    FROM vs_group s
    LEFT JOIN support_agents su ON su.id = s.agent_id
),
with_details AS (
    -- Attach agent attributes
    SELECT s.*, su.team AS agent_team
    FROM with_parent_agent s
    LEFT JOIN support_agents su ON su.id = s.agent_id
),
grouped AS (
    -- Group-level totals
    SELECT
        TO_DATE(opened_at) AS opened_at_day,
        closed_at_hour,
        agent_id,
        SUM(requester_id) AS total_requester_id,
        ROUND(SUM(ticket_priority_count_minus_avg) / NULLIF(SUM(requester_id), 0), 3) AS ticket_priority_count_minus_avg_to_requester_id_,
        ARRAY_SORT(COLLECT_SET(agent_name)) AS agent_name_list,
        ROUND(AVG(ticket_priority_count_minus_avg), 2) AS avg_ticket_priority_count_minus_avg
    FROM with_details
    GROUP BY TO_DATE(opened_at), closed_at_hour, agent_id
)
-- Native SQL UNPIVOT (wide to long)
SELECT opened_at_day, closed_at_hour, field, metric_value
FROM (
    SELECT opened_at_day, closed_at_hour, avg_ticket_priority_count_minus_avg, ticket_priority_count_minus_avg_to_requester_id_
    FROM grouped
) src
UNPIVOT (
    metric_value FOR field IN (avg_ticket_priority_count_minus_avg, ticket_priority_count_minus_avg_to_requester_id_)
) unp
