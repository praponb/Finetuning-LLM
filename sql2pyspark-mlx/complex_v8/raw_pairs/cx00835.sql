WITH valid AS (
    -- Scope the tickets rows
    SELECT
        id,
        requester_id,
        agent_id,
        priority,
        tags,
        SUBSTRING(TRIM(channel), 1, 4) AS channel_prefix,
        ROUND(csat / NULLIF(requester_id, 0), 3) AS csat_per_requester_id,
        ROUND(csat / NULLIF(requester_id, 0), 3) AS csat_per_requester_id_2
    FROM tickets
    WHERE csat <= 4
      AND priority IS NOT NULL
),
joined_agent AS (
    -- Enrich with dimension data
    SELECT s.*, su.hired_on AS agent_hired_on
    FROM valid s
    JOIN support_agents su ON su.id = s.agent_id
),
filtered AS (
    -- Keep rows whose agent matches
    SELECT s.*
    FROM joined_agent s
    WHERE NOT EXISTS (
            SELECT 1
            FROM support_agents su
            WHERE su.id = s.agent_id
              AND su.team = 'Onboarding'
              AND LOWER(su.agent_name) LIKE '%at%'
        )
),
without_matches AS (
    -- Keep rows whose agent matches
    SELECT s.*
    FROM filtered s
    WHERE EXISTS (
            SELECT 1
            FROM support_agents su
            WHERE su.id = s.agent_id
              AND su.tier IN ('T2', 'T1')
        )
)
SELECT *
FROM (
    SELECT agent_id, agent_hired_on, priority, csat_per_requester_id
    FROM without_matches
) src
PIVOT (
    SUM(csat_per_requester_id) AS total, MAX(csat_per_requester_id) AS max_v
    FOR priority IN ('P3', 'P1', 'P2')
)
