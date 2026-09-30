WITH filtered_tickets AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        requester_id,
        agent_id,
        channel,
        tags,
        COALESCE(csat, 0) AS csat_filled,
        requester_id + csat AS requester_id_plus_csat
    FROM tickets
    WHERE SIZE(tags) >= 1
),
combined_details AS (
    -- Join support_agents
    SELECT
        s.id,
        s.requester_id,
        s.agent_id,
        s.channel,
        s.tags,
        su.hired_on AS agent_hired_on,
        su.tier AS agent_tier
    FROM filtered_tickets s
    JOIN support_agents su ON su.id = s.agent_id
),
with_trends AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        requester_id,
        agent_id,
        agent_tier,
        LEAD(agent_hired_on, 3) OVER (ORDER BY agent_hired_on DESC, id) AS next3_agent_hired_on,
        NTH_VALUE(requester_id, 2) OVER (ORDER BY agent_hired_on DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_requester_id,
        LAST_VALUE(agent_hired_on) IGNORE NULLS OVER (ORDER BY agent_hired_on DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_agent_hired_on,
        ROUND(requester_id / NULLIF(SUM(requester_id) OVER (PARTITION BY agent_tier, agent_id), 0), 4) AS requester_id_share_of_agent_tier_agent_id
    FROM combined_details
),
rollup_base AS (
    -- Aggregate with conditional metrics
    SELECT
        agent_id,
        YEAR(next3_agent_hired_on) AS next3_agent_hired_on_year,
        COUNT(DISTINCT agent_tier) AS distinct_agent_tiers,
        ROUND(SUM(requester_id_share_of_agent_tier_agent_id) / NULLIF(SUM(second_requester_id), 0), 3) AS requester_id_share_of_agent_tier_agent_id_to_sec,
        SUM(requester_id_share_of_agent_tier_agent_id) AS total_requester_id_share_of_agent_tier_agent_id,
        SUM(CASE WHEN agent_tier <> 'T1' THEN 1 ELSE 0 END) AS n_agent_tier_t1,
        COUNT(*) AS n_rows
    FROM with_trends
    WHERE last_known_agent_hired_on IS NOT NULL
    GROUP BY 1, 2
    HAVING COUNT(*) >= 2
),
labeled AS (
    -- Label and score rows
    SELECT
        *,
        FLOOR(n_rows / 2) * 2 AS n_rows_bucket,
        FLOOR(n_rows / 2) * 2 AS n_rows_bucket_2
    FROM rollup_base
),
qualified AS (
    -- Rows with no matching tickets
    SELECT s.*
    FROM labeled s
    WHERE NOT EXISTS (
            SELECT 1
            FROM tickets t
            WHERE t.agent_id = s.agent_id
              AND t.requester_id >= 7
        )
),
annotated AS (
    -- Derive helper columns
    SELECT
        *,
        CASE WHEN distinct_agent_tiers >= 7 THEN 'HIGH' WHEN distinct_agent_tiers >= 5 THEN 'MEDIUM' ELSE 'LOW' END AS distinct_agent_tiers_tier
    FROM qualified
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT n_agent_tier_t1, next3_agent_hired_on_year, distinct_agent_tiers_tier, distinct_agent_tiers
    FROM annotated
) src
PIVOT (
    SUM(distinct_agent_tiers)
    FOR distinct_agent_tiers_tier IN ('HIGH', 'LOW')
)
ORDER BY n_agent_tier_t1, next3_agent_hired_on_year
