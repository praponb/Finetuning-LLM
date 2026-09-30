WITH cleaned_tickets AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        closed_at,
        csat,
        CASE WHEN requester_id >= 18 THEN 'A' WHEN requester_id >= 7 THEN 'B' ELSE 'C' END AS requester_id_tier
    FROM tickets
    WHERE MONTH(closed_at) IN (1, 2)
),
enriched_agent AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.agent_id,
        s.opened_at,
        s.closed_at,
        su.hired_on AS agent_hired_on,
        su.tier AS agent_tier,
        su.team AS agent_team
    FROM cleaned_tickets s
    JOIN support_agents su ON su.id = s.agent_id
),
windowed AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROUND(CUME_DIST() OVER (PARTITION BY agent_tier ORDER BY closed_at DESC, id), 3) AS closed_at_cume_dist,
        ROUND((UNIX_TIMESTAMP(opened_at) - UNIX_TIMESTAMP(LAG(opened_at) OVER (ORDER BY agent_hired_on, id))) / 60, 1) AS minutes_since_prev_opened_at,
        DENSE_RANK() OVER w_all_by_opened_at_ties AS opened_at_dense_rank,
        DENSE_RANK() OVER w_agent_team_agent_tier_by_clo AS closed_at_dense_rank,
        FIRST_VALUE(opened_at) IGNORE NULLS OVER (PARTITION BY agent_tier ORDER BY closed_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_opened_at,
        LAG(closed_at) OVER w_agent_id_agent_team_by_opene AS prev_closed_at,
        ROUND(CUME_DIST() OVER (PARTITION BY agent_team, agent_tier ORDER BY closed_at, id), 3) AS closed_at_cume_dist_2,
        ROUND(CUME_DIST() OVER w_all_by_agent_hired_on_ties, 3) AS agent_hired_on_cume_dist,
        DATEDIFF(agent_hired_on, LAG(agent_hired_on) OVER (ORDER BY closed_at DESC, id)) AS days_since_prev_agent_hired_on,
        LAG(closed_at, 2) OVER (ORDER BY agent_hired_on, id) AS prev2_closed_at,
        LEAD(agent_hired_on) OVER (ORDER BY opened_at DESC, id) AS next_agent_hired_on,
        ROUND(PERCENT_RANK() OVER w_all_by_agent_hired_on, 3) AS agent_hired_on_percent_rank,
        LAG(agent_hired_on, 2) OVER (ORDER BY opened_at DESC, id) AS prev2_agent_hired_on,
        ROUND((UNIX_TIMESTAMP(closed_at) - UNIX_TIMESTAMP(LAG(closed_at) OVER (ORDER BY opened_at DESC, id))) / 60, 1) AS minutes_since_prev_closed_at
    FROM enriched_agent
    WINDOW w_all_by_opened_at_ties AS (ORDER BY opened_at DESC), w_agent_team_agent_tier_by_clo AS (PARTITION BY agent_team, agent_tier ORDER BY closed_at, id), w_agent_id_agent_team_by_opene AS (PARTITION BY agent_id, agent_team ORDER BY opened_at, id), w_all_by_agent_hired_on_ties AS (ORDER BY agent_hired_on), w_all_by_agent_hired_on AS (ORDER BY agent_hired_on, id), w_agent_tier_by_closed_at AS (PARTITION BY agent_tier ORDER BY closed_at DESC, id)
)
-- Final projection
SELECT
    id,
    agent_id,
    opened_at,
    closed_at,
    agent_tier,
    closed_at_cume_dist,
    minutes_since_prev_opened_at,
    closed_at_dense_rank,
    prev_closed_at,
    closed_at_cume_dist_2,
    agent_hired_on_cume_dist,
    prev2_closed_at,
    next_agent_hired_on,
    agent_hired_on_percent_rank,
    prev2_agent_hired_on,
    minutes_since_prev_closed_at,
    CONCAT(agent_team, '#', CAST(agent_id AS STRING)) AS agent_team_ref,
    GREATEST(agent_hired_on_cume_dist, opened_at_dense_rank) AS greatest_agent_hired_on_cume_dist_opened_at_dens
FROM windowed
WHERE HOUR(opened_at) BETWEEN 9 AND 13
ORDER BY opened_at DESC, agent_team_ref, id
LIMIT 10
