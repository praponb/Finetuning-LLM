WITH base_agents AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        agent_name,
        team,
        tier,
        COALESCE(team, 'UNKNOWN') AS team_or_unknown
    FROM support_agents
    WHERE YEAR(hired_on) = 2025
      AND agent_name IS NOT NULL
),
ticket_stats AS (
    -- Per-agent tickets metrics
    SELECT agent_id, MAX(csat) AS max_ticket_csat
    FROM tickets
    GROUP BY agent_id
),
profiled AS (
    -- Attach tickets metrics
    SELECT
        s.*,
        t.max_ticket_csat
    FROM base_agents s
    LEFT JOIN ticket_stats t ON t.agent_id = s.id
),
windowed AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        team,
        tier,
        team_or_unknown,
        max_ticket_csat,
        ROUND(max_ticket_csat / NULLIF(SUM(max_ticket_csat) OVER (PARTITION BY team_or_unknown), 0), 4) AS max_ticket_csat_share_of_team_or_unknown,
        RANK() OVER (PARTITION BY tier, max_ticket_csat ORDER BY max_ticket_csat ASC NULLS LAST) AS max_ticket_csat_rank,
        DENSE_RANK() OVER (PARTITION BY tier, max_ticket_csat ORDER BY max_ticket_csat ASC NULLS LAST) AS max_ticket_csat_dense_rank
    FROM profiled
)
-- Final projection
SELECT
    id,
    max_ticket_csat,
    max_ticket_csat_share_of_team_or_unknown,
    max_ticket_csat_dense_rank
FROM windowed
WHERE max_ticket_csat_share_of_team_or_unknown > 0.1
ORDER BY max_ticket_csat_dense_rank, id
LIMIT 20
