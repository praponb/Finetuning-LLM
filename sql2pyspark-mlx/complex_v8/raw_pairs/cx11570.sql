WITH valid AS (
    -- Filter tickets
    SELECT
        id,
        agent_id,
        opened_at,
        closed_at,
        priority,
        channel,
        csat,
        CONCAT_WS('-', channel, priority) AS channel_priority_label,
        CASE WHEN priority IN ('P3', 'P1') THEN 'group_a' WHEN priority = 'P2' THEN 'group_b' ELSE 'other' END AS priority_group,
        DATEDIFF(DATE '2026-01-01', closed_at) AS days_since_closed_at
    FROM tickets
),
with_parent AS (
    -- Join support_agents
    SELECT
        s.id,
        s.agent_id,
        s.opened_at,
        s.priority,
        s.channel,
        s.csat,
        s.channel_priority_label,
        s.days_since_closed_at,
        su.hired_on AS agent_hired_on,
        su.tier AS agent_tier
    FROM valid s
    JOIN support_agents su ON su.id = s.agent_id
),
derived AS (
    -- Add calculated columns
    SELECT *, QUARTER(opened_at) AS opened_at_quarter
    FROM with_parent
)
-- Final projection
SELECT
    id,
    agent_id,
    opened_at,
    priority,
    channel,
    csat,
    channel_priority_label,
    agent_hired_on,
    agent_tier,
    opened_at_quarter,
    GREATEST(opened_at_quarter, csat) AS greatest_opened_at_quarter_csat,
    opened_at_quarter - days_since_closed_at AS opened_at_quarter_minus_days_since_closed_at
FROM derived
ORDER BY opened_at_quarter DESC, agent_id DESC
