WITH recent_tickets AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        channel,
        DATE_FORMAT(closed_at, 'yyyy-MM-dd') AS closed_at_label
    FROM tickets
    WHERE requester_id < 22
),
with_stats AS (
    -- Attach tickets metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FROM tickets t WHERE t.agent_id = s.agent_id) AS ticket_count
    FROM recent_tickets s
),
derived AS (
    -- Derive helper columns
    SELECT
        *,
        CONCAT(closed_at_label, '#', CAST(ticket_count AS STRING)) AS closed_at_label_ref,
        ABS(ticket_count - 5) AS ticket_count_distance,
        SUBSTRING(TRIM(channel), 1, 4) AS channel_prefix
    FROM with_stats
),
joined_agent AS (
    -- Join support_agents
    SELECT
        s.id,
        s.requester_id,
        s.agent_id,
        s.channel,
        s.closed_at_label,
        s.closed_at_label_ref,
        s.channel_prefix,
        su.team AS agent_team,
        su.hired_on AS agent_hired_on
    FROM derived s
    LEFT JOIN support_agents su ON su.id = s.agent_id
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT agent_id, agent_hired_on, channel, requester_id
    FROM joined_agent
) src
PIVOT (
    SUM(requester_id)
    FOR channel IN ('chat', 'web', 'email')
)
ORDER BY agent_id, agent_hired_on
