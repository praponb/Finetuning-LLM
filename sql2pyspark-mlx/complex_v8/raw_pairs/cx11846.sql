WITH cleaned AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        closed_at,
        CONCAT(priority, '#', CAST(agent_id AS STRING)) AS priority_ref
    FROM tickets
    WHERE (csat <= 4 OR requester_id >= 7)
),
combined_details_agent AS (
    -- Attach agent attributes
    SELECT
        s.id,
        s.requester_id,
        s.agent_id,
        s.opened_at,
        s.closed_at,
        s.priority_ref,
        su.hired_on AS agent_hired_on,
        su.team AS agent_team
    FROM cleaned s
    LEFT JOIN support_agents su ON su.id = s.agent_id
),
sequenced AS (
    -- Previous / next values per key
    SELECT
        id,
        agent_id,
        opened_at,
        closed_at,
        priority_ref,
        agent_hired_on,
        SIZE(COLLECT_SET(agent_id) OVER w_agent_team) AS distinct_agent_id_in_agent_team,
        SUM(requester_id) OVER (PARTITION BY agent_team, YEAR(agent_hired_on) ORDER BY requester_id DESC, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS requester_id_sum_4,
        ROUND(requester_id - AVG(requester_id) OVER (PARTITION BY agent_team, MONTH(closed_at)), 2) AS requester_id_vs_agent_team_closed_at_month_avg,
        MIN(requester_id) OVER (PARTITION BY agent_team) AS agent_team_min_requester_id,
        DENSE_RANK() OVER (PARTITION BY agent_team, YEAR(agent_hired_on) ORDER BY requester_id DESC, id) AS requester_id_dense_rank,
        ROUND(AVG(requester_id) OVER (PARTITION BY agent_team ORDER BY agent_hired_on, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS requester_id_avg_7,
        NTILE(4) OVER (ORDER BY requester_id NULLS LAST, id) AS requester_id_quartile
    FROM combined_details_agent
    WINDOW w_agent_team AS (PARTITION BY agent_team)
)
SELECT
    id,
    closed_at,
    priority_ref,
    agent_hired_on,
    distinct_agent_id_in_agent_team,
    requester_id_vs_agent_team_closed_at_month_avg,
    agent_team_min_requester_id,
    requester_id_dense_rank,
    requester_id_avg_7,
    requester_id_quartile
FROM sequenced
ORDER BY requester_id_avg_7 DESC NULLS FIRST, requester_id_quartile
