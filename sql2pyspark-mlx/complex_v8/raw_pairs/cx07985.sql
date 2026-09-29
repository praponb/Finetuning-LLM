WITH recent AS (
    -- Filter tickets
    SELECT
        id,
        agent_id,
        opened_at,
        priority,
        tags,
        SUBSTRING(TRIM(channel), 1, 3) AS channel_prefix,
        ROUND(csat / NULLIF(requester_id, 0), 3) AS csat_per_requester_id,
        CASE WHEN requester_id >= 22 THEN 'large' WHEN requester_id >= 7 THEN 'medium' ELSE 'small' END AS requester_id_tier
    FROM tickets
),
with_activity AS (
    -- Rows with no matching tickets
    SELECT s.*
    FROM recent s
    WHERE NOT EXISTS (
            SELECT 1
            FROM tickets t
            WHERE t.agent_id = s.agent_id
              AND SIZE(t.tags) >= 1
              AND t.requester_id > 8
        )
      AND NOT ARRAY_CONTAINS(s.tags, 'upgrade')
),
rollup_base AS (
    -- Group-level totals
    SELECT
        priority,
        COUNT(*) AS n_rows,
        COUNT(DISTINCT id) AS distinct_ids,
        COUNT(DISTINCT agent_id) AS distinct_agent_ids,
        MIN(opened_at) AS first_opened_at
    FROM with_activity
    GROUP BY 1
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        FIRST_VALUE(first_opened_at) OVER (PARTITION BY distinct_agent_ids, DATE_TRUNC('MONTH', first_opened_at) ORDER BY distinct_agent_ids, priority ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_first_opened_at,
        SIZE(COLLECT_SET(priority) OVER w_distinct_ids_distinct_agent) AS distinct_priority_in_distinct_ids_distinct_agent,
        MAX(n_rows) OVER (PARTITION BY distinct_agent_ids, DATE_TRUNC('MONTH', first_opened_at) ORDER BY distinct_agent_ids, priority ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_n_rows_so_far,
        COUNT(*) OVER (PARTITION BY distinct_ids, distinct_agent_ids) AS rows_in_distinct_ids_distinct_agent_ids,
        LEAD(n_rows, 2) OVER w_priority_n_rows_by_distinct AS next2_n_rows
    FROM rollup_base
    WINDOW w_distinct_ids_distinct_agent AS (PARTITION BY distinct_ids, distinct_agent_ids), w_priority_n_rows_by_distinct AS (PARTITION BY priority, n_rows ORDER BY distinct_ids ASC, priority)
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT distinct_agent_ids, next2_n_rows, priority, max_n_rows_so_far
    FROM metrics
) src
PIVOT (
    COUNT(max_n_rows_so_far) AS count_v, MAX(max_n_rows_so_far) AS max_v
    FOR priority IN ('P4', 'P1', 'P3')
)
ORDER BY distinct_agent_ids, next2_n_rows
