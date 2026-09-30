WITH base_tickets AS (
    -- Scope the tickets rows
    SELECT
        id,
        requester_id,
        agent_id,
        closed_at,
        channel
    FROM tickets
    WHERE channel = 'phone'
      AND HOUR(opened_at) BETWEEN 10 AND 16
),
combined_details_agent AS (
    -- Join support_agents
    SELECT
        s.id,
        s.agent_id,
        s.closed_at,
        s.channel,
        su.tier AS agent_tier,
        su.team AS agent_team,
        su.hired_on AS agent_hired_on
    FROM base_tickets s
    JOIN support_agents su ON su.id = s.agent_id
),
with_details AS (
    -- Join support_agents
    SELECT
        s.id,
        s.agent_id,
        s.closed_at,
        s.agent_tier,
        s.agent_team,
        s.agent_hired_on,
        su.tier AS agent_tier_2
    FROM combined_details_agent s
    JOIN support_agents su ON su.id = s.agent_id
    WHERE su.tier NOT IN ('T3', 'T1')
),
metrics AS (
    -- Window metrics over several partitions
    SELECT
        id,
        agent_id,
        closed_at,
        agent_tier,
        agent_team,
        agent_hired_on,
        agent_tier_2,
        ROUND((UNIX_TIMESTAMP(closed_at) - UNIX_TIMESTAMP(LAG(closed_at) OVER (ORDER BY agent_hired_on, id))) / 60, 1) AS minutes_since_prev_closed_at,
        ROW_NUMBER() OVER w_all_by_agent_hired_on_2 AS seq_no,
        LAG(closed_at) OVER (ORDER BY agent_hired_on DESC, id) AS prev_closed_at,
        NTH_VALUE(closed_at, 2) OVER (ORDER BY agent_hired_on DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_closed_at,
        ROW_NUMBER() OVER (ORDER BY agent_hired_on, id) AS agent_hired_on_seq
    FROM with_details
    WINDOW w_all_by_agent_hired_on_2 AS (ORDER BY agent_hired_on DESC, id)
),
annotated AS (
    -- Derive helper columns
    SELECT
        id,
        closed_at,
        agent_tier,
        agent_team,
        agent_hired_on,
        agent_tier_2,
        minutes_since_prev_closed_at,
        second_closed_at,
        agent_hired_on_seq,
        DATE_FORMAT(second_closed_at, 'MMM yyyy') AS second_closed_at_label
    FROM metrics
    WHERE agent_tier NOT IN ('T3', 'T2')
)
SELECT
    agent_team,
    agent_tier_2,
    agent_tier,
    GROUPING(agent_team) AS is_total_agent_team,
    ROUND(SUM(minutes_since_prev_closed_at), 2) AS total_minutes_since_prev_closed_at,
    MIN(closed_at) AS first_closed_at,
    ARRAY_SORT(COLLECT_SET(agent_team)) AS agent_team_list
FROM annotated
GROUP BY ROLLUP(agent_team, agent_tier_2, agent_tier)
ORDER BY agent_team NULLS LAST, agent_tier_2 NULLS LAST, agent_tier NULLS LAST
