WITH scoped AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        priority,
        channel,
        tags,
        CONCAT_WS(' / ', priority, channel) AS priority_channel_label,
        LEAST(requester_id, csat) AS least_requester_id_csat,
        DAYOFWEEK(closed_at) AS closed_at_dayofweek
    FROM tickets
    WHERE channel IS NOT NULL
),
ranked AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        SUM(requester_id) OVER (PARTITION BY agent_id, closed_at_dayofweek) AS agent_id_closed_at_dayofweek_sum_requester_id,
        NTH_VALUE(closed_at_dayofweek, 2) OVER (PARTITION BY closed_at_dayofweek, TO_DATE(opened_at) ORDER BY closed_at_dayofweek ASC NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_closed_at_dayofweek,
        FIRST_VALUE(opened_at) OVER (PARTITION BY closed_at_dayofweek, TO_DATE(opened_at) ORDER BY closed_at_dayofweek ASC NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_opened_at,
        ROUND(closed_at_dayofweek - AVG(closed_at_dayofweek) OVER w_agent_id_closed_at_dayofweek, 2) AS closed_at_dayofweek_vs_agent_id_closed_at_dayofw
    FROM scoped
    WINDOW w_agent_id_closed_at_dayofweek AS (PARTITION BY agent_id, closed_at_dayofweek)
),
with_parent_agent AS (
    -- Join support_agents
    SELECT
        s.id,
        s.requester_id,
        s.agent_id,
        s.priority,
        s.channel,
        s.tags,
        s.priority_channel_label,
        s.closed_at_dayofweek,
        s.agent_id_closed_at_dayofweek_sum_requester_id,
        s.second_closed_at_dayofweek,
        s.first_opened_at,
        su.agent_name,
        su.team AS agent_team,
        su.tier AS agent_tier
    FROM ranked s
    JOIN support_agents su ON su.id = s.agent_id
    WHERE su.team NOT IN ('Tech', 'Billing')
),
annotated AS (
    -- Add calculated columns
    SELECT
        *,
        requester_id - closed_at_dayofweek AS requester_id_minus_closed_at_dayofweek,
        ARRAY_JOIN(ARRAY_SORT(tags), ',') AS tags_text
    FROM with_parent_agent
    WHERE channel <> 'phone'
)
-- Subtotals with CUBE
SELECT
    closed_at_dayofweek,
    agent_id,
    COALESCE(agent_tier, 'ALL') AS agent_tier,
    GROUPING(closed_at_dayofweek) AS is_total_closed_at_dayofweek,
    SUM(agent_id_closed_at_dayofweek_sum_requester_id) FILTER (WHERE agent_tier IN ('T1', 'T3')) AS agent_id_closed_at_dayofweek_sum_requester_id_ag
FROM annotated
GROUP BY CUBE(closed_at_dayofweek, agent_id, agent_tier)
ORDER BY closed_at_dayofweek NULLS LAST, agent_id NULLS LAST, agent_tier NULLS LAST
