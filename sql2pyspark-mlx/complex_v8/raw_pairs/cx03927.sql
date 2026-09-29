WITH scoped_tickets AS (
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
        SUBSTRING(TRIM(channel), 1, 3) AS channel_prefix,
        DATEDIFF(DATE '2025-06-30', closed_at) AS days_since_closed_at,
        DATE_FORMAT(opened_at, 'yyyy-MM-dd') AS opened_at_label
    FROM tickets
    WHERE priority <> 'P2'
),
with_windows AS (
    -- Previous / next values per key
    SELECT
        *,
        ROUND(AVG(requester_id) OVER (PARTITION BY agent_id), 2) AS agent_id_avg_requester_id,
        ROUND(csat - AVG(csat) OVER (PARTITION BY agent_id), 2) AS csat_vs_agent_id_avg,
        ROW_NUMBER() OVER (PARTITION BY priority ORDER BY closed_at DESC, id) AS seq_no,
        SUM(csat) OVER (PARTITION BY csat, agent_id ORDER BY requester_id, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_csat,
        MAX(csat) OVER (PARTITION BY agent_id) AS agent_id_max_csat,
        LAG(csat, 3) OVER (PARTITION BY csat, agent_id ORDER BY requester_id, id) AS prev3_csat
    FROM scoped_tickets
),
combined_details AS (
    -- Join support_agents
    SELECT s.*, su.tier AS agent_tier
    FROM with_windows s
    JOIN support_agents su ON su.id = s.agent_id
)
SELECT
    agent_id,
    COALESCE(priority, 'ALL') AS priority,
    COALESCE(agent_tier, 'ALL') AS agent_tier,
    GROUPING_ID() AS gid,
    SUM(CASE WHEN channel = 'phone' THEN 1 ELSE 0 END) AS n_channel_phone,
    MAX(agent_id_max_csat) AS max_agent_id_max_csat
FROM combined_details
GROUP BY GROUPING SETS ((agent_id, priority, agent_tier), (agent_id), (priority))
ORDER BY agent_id NULLS LAST, priority NULLS LAST, agent_tier NULLS LAST
