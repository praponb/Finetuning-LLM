WITH cleaned AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        agent_id,
        opened_at,
        closed_at,
        priority,
        channel,
        csat,
        tags,
        CONCAT(priority, '#', CAST(agent_id AS STRING)) AS priority_ref,
        LOWER(REGEXP_REPLACE(TRIM(priority), ' ', '_')) AS priority_slug
    FROM tickets
    WHERE priority IS NOT NULL
      AND csat >= 2
),
with_windows AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        NTILE(5) OVER w_csat_agent_id_by_csat AS csat_quintile,
        LAST_VALUE(closed_at) OVER (PARTITION BY csat, agent_id ORDER BY csat DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_closed_at,
        ROUND(PERCENT_RANK() OVER w_csat_agent_id_by_csat_ties, 3) AS csat_percent_rank,
        MAX(csat) OVER (PARTITION BY csat) AS csat_max_csat
    FROM cleaned
    WINDOW w_csat_agent_id_by_csat AS (PARTITION BY csat, agent_id ORDER BY csat DESC, id), w_csat_agent_id_by_csat_ties AS (PARTITION BY csat, agent_id ORDER BY csat DESC)
)
SELECT *
FROM (
    SELECT last_closed_at, agent_id, channel, csat_max_csat
    FROM with_windows
) src
PIVOT (
    SUM(csat_max_csat)
    FOR channel IN ('chat', 'web')
)
ORDER BY last_closed_at, agent_id
