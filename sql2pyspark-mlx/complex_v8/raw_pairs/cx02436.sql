WITH cleaned AS (
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
        DATE_FORMAT(opened_at, 'yyyy-MM-dd') AS opened_at_label,
        DATE_TRUNC('YEAR', closed_at) AS closed_at_year
    FROM tickets
    WHERE (TO_DATE(opened_at) < DATE '2025-03-08' OR priority = 'P4')
),
joined AS (
    -- Join support_agents
    SELECT
        s.*,
        su.agent_name,
        su.hired_on AS agent_hired_on,
        su.tier AS agent_tier
    FROM cleaned s
    LEFT JOIN support_agents su ON su.id = s.agent_id
),
with_details AS (
    -- Join support_agents
    SELECT
        s.id,
        s.requester_id,
        s.agent_id,
        s.closed_at,
        s.priority,
        s.channel,
        s.tags,
        s.opened_at_label,
        s.closed_at_year,
        s.agent_name,
        s.agent_hired_on,
        su.tier AS agent_tier_2
    FROM joined s
    JOIN support_agents su ON su.id = s.agent_id
    WHERE su.tier <> 'T3'
),
totals AS (
    -- Summarise per group
    SELECT
        DATE_TRUNC('MONTH', agent_hired_on) AS agent_hired_on_month,
        ROUND(STDDEV(requester_id), 2) AS stddev_requester_id,
        ARRAY_SORT(COLLECT_SET(agent_name)) AS agent_name_list,
        ARRAY_SORT(COLLECT_SET(agent_tier_2)) AS agent_tier_2_list,
        COUNT(*) FILTER (WHERE channel = 'web') AS n_channel_web,
        ROUND(100 * COUNT(CASE WHEN LENGTH(TRIM(agent_name)) >= 8 THEN 1 END) / COUNT(*), 1) AS pct_length_trim_agent_name_8,
        MAX(requester_id) AS max_requester_id
    FROM with_details
    GROUP BY 1
    HAVING SUM(requester_id) > 7
),
derived AS (
    -- Label and score rows
    SELECT
        *,
        ROUND(n_channel_web / NULLIF(max_requester_id, 0), 3) AS n_channel_web_per_max_requester_id
    FROM totals
    WHERE SIZE(agent_tier_2_list) >= 1
)
-- Final projection
SELECT
    agent_hired_on_month,
    stddev_requester_id,
    agent_name_list,
    agent_tier_2_list,
    n_channel_web,
    pct_length_trim_agent_name_8,
    max_requester_id,
    n_channel_web_per_max_requester_id,
    CASE WHEN max_requester_id >= 15 THEN 'HIGH' WHEN max_requester_id >= 12 THEN 'MEDIUM' ELSE 'LOW' END AS max_requester_id_tier,
    MONTH(agent_hired_on_month) AS agent_hired_on_month_month
FROM derived
ORDER BY max_requester_id_tier DESC, n_channel_web_per_max_requester_id, agent_hired_on_month
LIMIT 5
