WITH recent_tickets AS (
    -- Filter tickets
    SELECT
        id,
        requester_id,
        agent_id,
        closed_at,
        priority,
        channel,
        csat,
        tags,
        COALESCE(requester_id, 0) AS requester_id_filled,
        requester_id + csat AS requester_id_plus_csat,
        CONCAT_WS(' / ', channel, priority) AS channel_priority_label
    FROM tickets
    WHERE (agent_id IS NOT NULL OR channel IS NOT NULL)
),
with_details_agent AS (
    -- Join support_agents
    SELECT
        s.id,
        s.requester_id,
        s.agent_id,
        s.closed_at,
        s.priority,
        s.channel,
        s.csat,
        s.tags,
        s.requester_id_filled,
        s.requester_id_plus_csat,
        s.channel_priority_label,
        su.team AS agent_team
    FROM recent_tickets s
    LEFT JOIN support_agents su ON su.id = s.agent_id
),
tagged AS (
    -- Add calculated columns
    SELECT
        *,
        YEAR(closed_at) AS closed_at_year,
        ABS(requester_id_filled - 12) AS requester_id_filled_distance
    FROM with_details_agent
),
with_windows AS (
    -- Window metrics over several partitions
    SELECT
        id,
        requester_id,
        agent_id,
        closed_at,
        priority,
        channel,
        csat,
        requester_id_plus_csat,
        NTILE(4) OVER (PARTITION BY priority ORDER BY csat NULLS LAST, id) AS csat_quartile,
        LAST_VALUE(csat) OVER (PARTITION BY csat ORDER BY csat ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_csat,
        MIN(csat) OVER (PARTITION BY priority) AS priority_min_csat,
        ROUND(AVG(closed_at_year) OVER (PARTITION BY priority ORDER BY csat NULLS LAST, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING), 2) AS closed_at_year_avg_1_prior,
        LEAD(closed_at_year, 1, 0) OVER w_priority_by_csat AS next_closed_at_year,
        ROUND(requester_id_filled - AVG(requester_id_filled) OVER (PARTITION BY priority), 2) AS requester_id_filled_vs_priority_avg
    FROM tagged
    WINDOW w_priority_by_csat AS (PARTITION BY priority ORDER BY csat NULLS LAST, id)
),
with_parent_agent AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.requester_id,
        s.agent_id,
        s.closed_at,
        s.priority,
        s.channel,
        s.csat,
        s.requester_id_plus_csat,
        s.last_csat,
        s.priority_min_csat,
        s.closed_at_year_avg_1_prior,
        s.next_closed_at_year,
        s.requester_id_filled_vs_priority_avg,
        su.hired_on AS agent_hired_on,
        su.tier AS agent_tier,
        su.agent_name
    FROM with_windows s
    JOIN support_agents su ON su.id = s.agent_id
)
SELECT
    id,
    requester_id,
    agent_id,
    priority,
    channel,
    csat,
    priority_min_csat,
    closed_at_year_avg_1_prior,
    next_closed_at_year,
    agent_tier,
    agent_name,
    DATEDIFF(DATE '2025-06-30', closed_at) AS days_since_closed_at
FROM with_parent_agent
ORDER BY closed_at_year_avg_1_prior NULLS LAST, next_closed_at_year DESC
