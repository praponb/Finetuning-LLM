WITH valid AS (
    -- Keep valid rows and derive helper columns
    SELECT *
    FROM tickets
    WHERE priority <> 'P2'
      AND HOUR(closed_at) BETWEEN 6 AND 11
),
combined_details_agent AS (
    -- Join support_agents
    SELECT
        s.id,
        s.requester_id,
        s.agent_id,
        s.opened_at,
        s.channel,
        s.csat,
        s.tags,
        su.team AS agent_team
    FROM valid s
    LEFT JOIN support_agents su ON su.id = s.agent_id AND su.tier IN ('T3', 'T2')
),
sequenced AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROUND(AVG(csat) OVER (PARTITION BY channel, csat ORDER BY opened_at, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2) AS csat_avg_3,
        ROUND(requester_id / NULLIF(SUM(requester_id) OVER (PARTITION BY agent_team), 0), 4) AS requester_id_share_of_agent_team,
        ROUND(csat / NULLIF(SUM(csat) OVER (PARTITION BY agent_team), 0), 4) AS csat_share_of_agent_team,
        FIRST_VALUE(requester_id) IGNORE NULLS OVER (PARTITION BY agent_id ORDER BY csat, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_requester_id
    FROM combined_details_agent
)
SELECT
    id,
    requester_id,
    opened_at,
    channel,
    csat,
    agent_team,
    requester_id_share_of_agent_team,
    csat_share_of_agent_team,
    first_known_requester_id,
    ABS(csat_share_of_agent_team - 0.6) AS csat_share_of_agent_team_distance,
    ROUND(first_known_requester_id / NULLIF(csat, 0), 3) AS first_known_requester_id_per_csat
FROM sequenced
ORDER BY agent_team DESC
