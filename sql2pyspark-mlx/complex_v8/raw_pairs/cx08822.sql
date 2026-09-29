WITH recent AS (
    -- Filter tickets
    SELECT
        id,
        requester_id,
        agent_id,
        priority,
        tags,
        LOWER(TRIM(priority)) AS priority_clean
    FROM tickets
    WHERE opened_at >= TIMESTAMP '2025-01-13 13:00:00'
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        requester_id,
        priority,
        tags,
        priority_clean,
        FIRST_VALUE(requester_id) IGNORE NULLS OVER (PARTITION BY agent_id, priority ORDER BY requester_id DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_requester_id,
        NTH_VALUE(requester_id, 2) OVER (PARTITION BY agent_id, priority ORDER BY requester_id DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_requester_id,
        NTH_VALUE(requester_id, 2) OVER (PARTITION BY agent_id ORDER BY requester_id DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_requester_id_2,
        MAX(requester_id) OVER (PARTITION BY agent_id ORDER BY requester_id DESC, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS requester_id_max_4
    FROM recent
),
priority_stats AS (
    -- Median second_requester_id per priority
    SELECT priority, PERCENTILE_APPROX(second_requester_id, 0.5) AS median_second_requester_id
    FROM metrics
    GROUP BY priority
),
outliers AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.median_second_requester_id,
        ROUND(s.second_requester_id - g.median_second_requester_id, 2) AS second_requester_id_minus_median
    FROM metrics s
    JOIN priority_stats g ON g.priority = s.priority
    WHERE s.second_requester_id >= g.median_second_requester_id
)
-- Native SQL UNPIVOT (wide to long)
SELECT id, field, reading
FROM (
    SELECT id, requester_id_max_4, first_known_requester_id, second_requester_id, requester_id
    FROM outliers
) src
UNPIVOT (
    reading FOR field IN (requester_id_max_4, first_known_requester_id, second_requester_id, requester_id)
) unp
ORDER BY id, field
