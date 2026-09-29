WITH base AS (
    -- Filter tickets
    SELECT
        id,
        agent_id,
        closed_at,
        priority,
        channel,
        tags,
        FLOOR(requester_id / 5) * 5 AS requester_id_bucket,
        SUBSTRING(TRIM(channel), 1, 4) AS channel_prefix,
        CONCAT(priority, '#', CAST(agent_id AS STRING)) AS priority_ref
    FROM tickets
    WHERE agent_id <= 3
      AND HOUR(closed_at) BETWEEN 6 AND 9
),
annotated AS (
    -- Derive helper columns
    SELECT
        *,
        COALESCE(channel, 'UNKNOWN') AS channel_or_unknown,
        CASE WHEN priority IN ('P2', 'P4') THEN 'group_a' WHEN priority = 'P1' THEN 'group_b' ELSE 'other' END AS priority_group,
        SUBSTRING(TRIM(channel), 1, 2) AS channel_prefix_2
    FROM base
    WHERE requester_id_bucket BETWEEN 3 AND 20
),
ranked AS (
    -- Previous / next values per key
    SELECT
        *,
        ROUND(CUME_DIST() OVER (ORDER BY requester_id_bucket, id), 3) AS requester_id_bucket_cume_dist,
        ROUND(requester_id_bucket - AVG(requester_id_bucket) OVER (PARTITION BY channel_or_unknown, priority), 2) AS requester_id_bucket_vs_channel_or_unknown_priori,
        LAG(closed_at, 2) OVER (PARTITION BY priority_group ORDER BY closed_at, id) AS prev2_closed_at,
        LAG(closed_at) OVER w_all_by_closed_at AS prev_closed_at
    FROM annotated
    WINDOW w_all_by_closed_at AS (ORDER BY closed_at, id)
),
ranked_2 AS (
    -- Running and moving aggregates
    SELECT
        id,
        priority,
        channel,
        tags,
        requester_id_bucket,
        channel_prefix,
        channel_or_unknown,
        priority_group,
        channel_prefix_2,
        requester_id_bucket_cume_dist,
        ROUND(AVG(requester_id_bucket) OVER w_priority_group_priority, 2) AS priority_group_priority_avg_requester_id_bucket,
        NTILE(5) OVER w_channel_by_prev_closed_at AS prev_closed_at_quintile,
        SIZE(COLLECT_SET(channel) OVER w_priority_group_priority) AS distinct_channel_in_priority_group_priority,
        NTILE(10) OVER w_all_by_prev2_closed_at AS prev2_closed_at_decile,
        LEAD(closed_at, 2) OVER w_all_by_prev2_closed_at AS next2_closed_at
    FROM ranked
    WINDOW w_priority_group_priority AS (PARTITION BY priority_group, priority), w_channel_by_prev_closed_at AS (PARTITION BY channel ORDER BY prev_closed_at DESC, id), w_all_by_prev2_closed_at AS (ORDER BY prev2_closed_at DESC, id)
)
-- Subtotals with ROLLUP
SELECT
    prev2_closed_at_decile,
    priority,
    SUM(prev_closed_at_quintile) AS total_prev_closed_at_quintile
FROM ranked_2
GROUP BY ROLLUP(prev2_closed_at_decile, priority)
ORDER BY prev2_closed_at_decile NULLS LAST, priority NULLS LAST
