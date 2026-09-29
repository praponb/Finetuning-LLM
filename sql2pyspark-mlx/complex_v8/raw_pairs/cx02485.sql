WITH scoped AS (
    -- Scope the tickets rows
    SELECT
        id,
        requester_id,
        agent_id,
        closed_at,
        priority,
        channel,
        COALESCE(priority, 'UNKNOWN') AS priority_or_unknown,
        ABS(csat - 4) AS csat_distance
    FROM tickets
    WHERE agent_id IS NOT NULL
      AND ARRAY_CONTAINS(tags, 'upgrade')
),
with_windows AS (
    -- Window metrics over several partitions
    SELECT
        id,
        requester_id,
        agent_id,
        closed_at,
        priority,
        channel,
        csat_distance,
        SUM(csat_distance) OVER (PARTITION BY priority, priority_or_unknown ORDER BY requester_id DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_csat_distance,
        MIN(requester_id) OVER (PARTITION BY priority, priority_or_unknown ORDER BY requester_id DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_requester_id_so_far,
        requester_id - LAG(requester_id) OVER (PARTITION BY priority, channel ORDER BY requester_id DESC, id) AS requester_id_change,
        MIN(csat_distance) OVER w_csat_distance_priority_or_un AS csat_distance_priority_or_unknown_min_csat_dista,
        MAX(csat_distance) OVER (PARTITION BY agent_id, TO_DATE(closed_at)) AS agent_id_closed_at_to_date_max_csat_distance,
        DENSE_RANK() OVER (PARTITION BY priority, priority_or_unknown ORDER BY requester_id DESC, id) AS requester_id_dense_rank,
        SUM(requester_id) OVER (PARTITION BY priority, priority_or_unknown ORDER BY requester_id DESC, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS requester_id_sum_7,
        SIZE(COLLECT_SET(channel) OVER (PARTITION BY agent_id, TO_DATE(closed_at))) AS distinct_channel_in_agent_id_closed_at_to_date,
        MAX(csat_distance) OVER (PARTITION BY csat_distance, priority_or_unknown) AS csat_distance_priority_or_unknown_max_csat_dista,
        ROUND(AVG(requester_id) OVER (PARTITION BY agent_id, TO_DATE(closed_at)), 2) AS agent_id_closed_at_to_date_avg_requester_id,
        LEAD(requester_id) OVER (PARTITION BY priority_or_unknown ORDER BY csat_distance DESC, id) AS next_requester_id,
        NTH_VALUE(csat_distance, 2) OVER (ORDER BY closed_at NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_csat_distance,
        ROUND(AVG(csat_distance) OVER (PARTITION BY priority, channel ORDER BY requester_id DESC, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2) AS csat_distance_avg_3,
        LEAD(requester_id) OVER (PARTITION BY priority, channel ORDER BY requester_id DESC, id) AS next_requester_id_2,
        ROW_NUMBER() OVER (PARTITION BY priority, channel ORDER BY requester_id DESC, id) AS requester_id_seq
    FROM scoped
    WINDOW w_csat_distance_priority_or_un AS (PARTITION BY csat_distance, priority_or_unknown)
),
sequenced AS (
    -- Running and moving aggregates
    SELECT
        id,
        requester_id,
        agent_id,
        closed_at,
        priority,
        channel,
        min_requester_id_so_far,
        csat_distance_priority_or_unknown_min_csat_dista,
        requester_id_dense_rank,
        distinct_channel_in_agent_id_closed_at_to_date,
        csat_distance_priority_or_unknown_max_csat_dista,
        next_requester_id,
        second_csat_distance,
        next_requester_id_2,
        requester_id_seq,
        SIZE(COLLECT_SET(channel) OVER w_agent_id) AS distinct_channel_in_agent_id,
        ROUND(CUME_DIST() OVER (ORDER BY distinct_channel_in_agent_id_closed_at_to_date ASC), 3) AS distinct_channel_in_agent_id_closed_at_to_date_c,
        DENSE_RANK() OVER (PARTITION BY channel ORDER BY requester_id_change, id) AS requester_id_change_dense_rank,
        MIN(csat_distance_avg_3) OVER w_agent_id AS agent_id_min_csat_distance_avg_3,
        requester_id_change - LAG(requester_id_change) OVER (ORDER BY distinct_channel_in_agent_id_closed_at_to_date ASC, id) AS requester_id_change_change,
        DENSE_RANK() OVER (PARTITION BY channel ORDER BY requester_id_change) AS requester_id_change_dense_rank_2
    FROM with_windows
    WINDOW w_agent_id AS (PARTITION BY agent_id)
),
summary AS (
    -- Group-level totals
    SELECT
        requester_id_dense_rank,
        SUM(requester_id_dense_rank) AS total_requester_id_dense_rank,
        SUM(CASE WHEN channel <> 'web' THEN 1 ELSE 0 END) AS n_channel_web,
        COUNT(*) FILTER (WHERE priority <> 'P1') AS n_priority_p1,
        ROUND(AVG(min_requester_id_so_far), 2) AS avg_min_requester_id_so_far,
        ROUND(STDDEV(next_requester_id_2), 2) AS stddev_next_requester_id_2
    FROM sequenced
    GROUP BY requester_id_dense_rank
)
-- Final projection
SELECT
    requester_id_dense_rank,
    n_priority_p1,
    avg_min_requester_id_so_far,
    CASE WHEN requester_id_dense_rank >= 6 THEN 'large' WHEN requester_id_dense_rank >= 3 THEN 'medium' ELSE 'small' END AS requester_id_dense_rank_tier,
    ROUND(n_priority_p1 / NULLIF(n_channel_web, 0), 3) AS n_priority_p1_per_n_channel_web
FROM summary
WHERE total_requester_id_dense_rank >= 13
ORDER BY n_priority_p1 DESC, avg_min_requester_id_so_far NULLS LAST
