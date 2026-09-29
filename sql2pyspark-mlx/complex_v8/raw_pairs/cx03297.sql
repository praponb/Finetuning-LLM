WITH scoped AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        agent_id,
        priority,
        csat,
        tags,
        ROUND(csat / NULLIF(requester_id, 0), 3) AS csat_per_requester_id
    FROM tickets
    WHERE MONTH(opened_at) IN (2, 3)
      AND priority IN ('P4', 'P2', 'P3')
),
without_matches AS (
    -- Keep rows whose agent matches
    SELECT s.*
    FROM scoped s
    WHERE EXISTS (
            SELECT 1
            FROM support_agents su
            WHERE su.id = s.agent_id
              AND su.tier = 'T3'
              AND su.hired_on IS NOT NULL
        )
),
filtered AS (
    -- Keep rows whose agent matches
    SELECT s.*
    FROM without_matches s
    WHERE NOT EXISTS (
            SELECT 1
            FROM support_agents su
            WHERE su.id = s.agent_id
              AND su.team IS NOT NULL
        )
),
scored AS (
    -- Add calculated columns
    SELECT
        id,
        agent_id,
        csat,
        tags,
        csat_per_requester_id,
        GREATEST(csat_per_requester_id, csat) AS greatest_csat_per_requester_id_csat,
        COALESCE(csat, 0) AS csat_filled
    FROM filtered
),
sequenced AS (
    -- Previous / next values per key
    SELECT
        id,
        agent_id,
        csat,
        tags,
        csat_per_requester_id,
        greatest_csat_per_requester_id_csat,
        csat_filled,
        LAST_VALUE(csat_filled) OVER (ORDER BY csat_per_requester_id DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_csat_filled,
        ROUND(PERCENT_RANK() OVER (PARTITION BY csat, csat_filled ORDER BY csat_filled DESC), 3) AS csat_filled_percent_rank
    FROM scored
)
-- Final projection
SELECT
    id,
    agent_id,
    csat_per_requester_id,
    greatest_csat_per_requester_id_csat,
    csat_filled,
    last_csat_filled,
    csat_filled_percent_rank,
    COALESCE(csat_per_requester_id, 0.0) AS csat_per_requester_id_filled
FROM sequenced
ORDER BY greatest_csat_per_requester_id_csat, csat_filled DESC
