WITH recent_tickets AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        priority,
        csat,
        requester_id + csat AS requester_id_plus_csat
    FROM tickets
    WHERE SIZE(tags) >= 1
),
with_trends AS (
    -- Running and moving aggregates
    SELECT
        id,
        requester_id,
        priority,
        csat,
        ROUND(requester_id_plus_csat / NULLIF(SUM(requester_id_plus_csat) OVER w_priority_agent_id, 0), 4) AS requester_id_plus_csat_share_of_priority_agent_i,
        csat - LAG(csat) OVER (PARTITION BY agent_id, priority ORDER BY opened_at, id) AS csat_change
    FROM recent_tickets
    WINDOW w_priority_agent_id AS (PARTITION BY priority, agent_id)
),
qualified AS (
    -- Compare against the overall max
    SELECT *
    FROM with_trends
    WHERE requester_id_plus_csat_share_of_priority_agent_i < (SELECT MAX(requester_id_plus_csat_share_of_priority_agent_i) FROM with_trends)
),
scored AS (
    -- Derive helper columns
    SELECT
        *,
        ROUND(csat_change / NULLIF(csat, 0), 3) AS csat_change_per_csat,
        LENGTH(TRIM(priority)) AS priority_len
    FROM qualified
    WHERE priority = 'P4'
),
priority_stats AS (
    -- Avg requester_id per priority
    SELECT priority, AVG(requester_id) AS avg_requester_id
    FROM scored
    GROUP BY priority
),
outliers AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_requester_id,
        ROUND(s.requester_id - g.avg_requester_id, 2) AS requester_id_minus_avg
    FROM scored s
    JOIN priority_stats g ON g.priority = s.priority
    WHERE s.requester_id > g.avg_requester_id
),
with_trends_2 AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        priority_len - LAG(priority_len) OVER (PARTITION BY csat, priority ORDER BY csat_change_per_csat DESC NULLS FIRST, id) AS priority_len_change,
        FIRST_VALUE(csat) IGNORE NULLS OVER (PARTITION BY csat, priority ORDER BY csat_change_per_csat DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_csat,
        ROUND(CUME_DIST() OVER (PARTITION BY csat, priority ORDER BY csat_change_per_csat DESC NULLS FIRST, id), 3) AS csat_change_per_csat_cume_dist
    FROM outliers
)
SELECT id, metric_name, metric_value
FROM (
    SELECT id, requester_id_minus_avg, requester_id_plus_csat_share_of_priority_agent_i, csat_change_per_csat
    FROM with_trends_2
) src
UNPIVOT (
    metric_value FOR metric_name IN (requester_id_minus_avg, requester_id_plus_csat_share_of_priority_agent_i, csat_change_per_csat)
) unp
