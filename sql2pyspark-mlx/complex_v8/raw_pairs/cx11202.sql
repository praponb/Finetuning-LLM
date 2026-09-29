WITH base AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        agent_id,
        opened_at,
        closed_at,
        priority,
        csat,
        CASE WHEN csat >= 4 THEN 'HIGH' WHEN csat >= 3 THEN 'MEDIUM' ELSE 'LOW' END AS csat_tier
    FROM tickets
    WHERE requester_id <= 18
      AND csat > 1
),
csat_tier_stats AS (
    -- Avg csat per csat_tier
    SELECT csat_tier, AVG(csat) AS avg_csat
    FROM base
    GROUP BY csat_tier
),
above_average AS (
    -- Rows compared with their group statistic
    SELECT s.*, g.avg_csat, ROUND(s.csat - g.avg_csat, 2) AS csat_minus_avg
    FROM base s
    JOIN csat_tier_stats g ON g.csat_tier = s.csat_tier
    WHERE s.csat > g.avg_csat
),
scored AS (
    -- Add calculated columns
    SELECT
        *,
        LOWER(REGEXP_REPLACE(TRIM(csat_tier), ' ', '_')) AS csat_tier_slug,
        LENGTH(TRIM(priority)) AS priority_len
    FROM above_average
),
qualified AS (
    -- Rows with no matching tickets
    SELECT s.*
    FROM scored s
    WHERE NOT EXISTS (
            SELECT 1
            FROM tickets t
            WHERE t.agent_id = s.agent_id
              AND t.csat > 3
        )
      AND s.csat >= 3
),
sequenced AS (
    -- Running and moving aggregates
    SELECT
        *,
        LAST_VALUE(csat) IGNORE NULLS OVER (PARTITION BY priority_len ORDER BY avg_csat ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_csat,
        MAX(priority_len) OVER w_csat_tier_agent_id AS csat_tier_agent_id_max_priority_len,
        MAX(avg_csat) OVER w_csat_tier_agent_id AS csat_tier_agent_id_max_avg_csat,
        ROUND(PERCENT_RANK() OVER (ORDER BY priority_len ASC), 3) AS priority_len_percent_rank,
        ROUND(AVG(priority_len) OVER w_csat_tier_agent_id, 2) AS csat_tier_agent_id_avg_priority_len,
        ROUND(PERCENT_RANK() OVER (ORDER BY priority_len ASC, id), 3) AS priority_len_percent_rank_2
    FROM qualified
    WINDOW w_csat_tier_agent_id AS (PARTITION BY csat_tier, agent_id)
),
qualified_2 AS (
    -- Rows that have matching tickets
    SELECT s.*
    FROM sequenced s
    WHERE EXISTS (
            SELECT 1
            FROM tickets t
            WHERE t.agent_id = s.agent_id
              AND TO_DATE(t.opened_at) < DATE '2025-02-12'
              AND SIZE(t.tags) >= 1
        )
),
with_parent_agent AS (
    -- Attach agent attributes
    SELECT
        s.id,
        s.agent_id,
        s.opened_at,
        s.closed_at,
        s.priority,
        s.csat,
        s.csat_tier,
        s.last_known_csat,
        s.csat_tier_agent_id_max_avg_csat,
        s.priority_len_percent_rank,
        s.priority_len_percent_rank_2,
        su.team AS agent_team,
        su.agent_name,
        su.tier AS agent_tier
    FROM qualified_2 s
    JOIN support_agents su ON su.id = s.agent_id
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT closed_at, agent_tier, csat_tier, priority_len_percent_rank
    FROM with_parent_agent
) src
PIVOT (
    SUM(priority_len_percent_rank)
    FOR csat_tier IN ('HIGH', 'LOW', 'MEDIUM')
)
