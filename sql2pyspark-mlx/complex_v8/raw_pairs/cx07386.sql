WITH cleaned AS (
    -- Scope the tickets rows
    SELECT
        id,
        requester_id,
        agent_id,
        priority,
        tags
    FROM tickets
    WHERE closed_at < TIMESTAMP '2025-02-21 07:00:00'
),
agent_id_stats AS (
    -- Max requester_id per agent_id
    SELECT agent_id, MAX(requester_id) AS max_requester_id
    FROM cleaned
    GROUP BY agent_id
),
outliers AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.max_requester_id,
        ROUND(s.requester_id - g.max_requester_id, 2) AS requester_id_minus_max
    FROM cleaned s
    JOIN agent_id_stats g ON g.agent_id = s.agent_id
    WHERE s.requester_id = g.max_requester_id
),
ranked AS (
    -- Running and moving aggregates
    SELECT
        *,
        NTILE(5) OVER (PARTITION BY priority, agent_id ORDER BY requester_id_minus_max, id) AS requester_id_minus_max_quintile,
        NTILE(10) OVER (ORDER BY max_requester_id DESC, id) AS max_requester_id_decile,
        LAST_VALUE(max_requester_id) IGNORE NULLS OVER (PARTITION BY agent_id ORDER BY requester_id, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_max_requester_id,
        LEAD(requester_id_minus_max, 1, 0) OVER (PARTITION BY priority, agent_id ORDER BY requester_id_minus_max, id) AS next_requester_id_minus_max,
        ROUND(AVG(requester_id) OVER (PARTITION BY agent_id ORDER BY requester_id, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS requester_id_avg_7
    FROM outliers
),
agent_id_stats_2 AS (
    -- Median max_requester_id_decile per agent_id
    SELECT agent_id, PERCENTILE_APPROX(max_requester_id_decile, 0.5) AS median_max_requester_id_decile
    FROM ranked
    GROUP BY agent_id
),
above_average AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.median_max_requester_id_decile,
        ROUND(s.max_requester_id_decile - g.median_max_requester_id_decile, 2) AS max_requester_id_decile_minus_median
    FROM ranked s
    JOIN agent_id_stats_2 g ON g.agent_id = s.agent_id
    WHERE s.max_requester_id_decile >= g.median_max_requester_id_decile
)
SELECT
    id,
    requester_id,
    agent_id,
    max_requester_id,
    requester_id_minus_max,
    requester_id_minus_max_quintile,
    max_requester_id_decile,
    next_requester_id_minus_max,
    requester_id_avg_7,
    median_max_requester_id_decile,
    max_requester_id_decile_minus_median,
    ABS(requester_id - 7) AS requester_id_distance
FROM above_average
ORDER BY max_requester_id_decile_minus_median, max_requester_id DESC, id
LIMIT 5
