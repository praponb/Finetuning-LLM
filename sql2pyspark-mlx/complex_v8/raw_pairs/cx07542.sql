WITH valid AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        tags,
        INITCAP(TRIM(priority)) AS priority_clean,
        CASE WHEN requester_id >= 18 THEN 'large' WHEN requester_id >= 8 THEN 'medium' ELSE 'small' END AS requester_id_tier
    FROM tickets
    WHERE agent_id IS NOT NULL
      AND ARRAY_CONTAINS(tags, 'upgrade')
),
without_matches AS (
    -- Keep rows whose agent matches
    SELECT s.*
    FROM valid s
    WHERE EXISTS (
            SELECT 1
            FROM support_agents su
            WHERE su.id = s.agent_id
              AND su.hired_on IS NOT NULL
              AND LOWER(su.agent_name) LIKE '%sa%'
        )
),
rollup_base AS (
    -- Group-level totals
    SELECT
        requester_id_tier,
        agent_id,
        COUNT(*) AS cnt,
        COUNT(DISTINCT id) AS distinct_ids,
        MAX(opened_at) AS last_opened_at,
        PERCENTILE_APPROX(requester_id, 0.5) AS median_requester_id,
        COUNT(DISTINCT agent_id) AS distinct_agent_ids
    FROM without_matches
    GROUP BY requester_id_tier, agent_id
    HAVING COUNT(*) >= 2
)
SELECT
    requester_id_tier,
    agent_id,
    distinct_ids,
    last_opened_at,
    median_requester_id,
    distinct_agent_ids,
    ROUND(median_requester_id / NULLIF(distinct_ids, 0), 3) AS median_requester_id_per_distinct_ids
FROM rollup_base
ORDER BY last_opened_at
