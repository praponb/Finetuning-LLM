WITH cleaned AS (
    -- Filter tickets
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        closed_at,
        priority,
        channel,
        tags,
        CASE WHEN priority IN ('P3', 'P4') THEN 'group_a' WHEN priority = 'P1' THEN 'group_b' ELSE 'other' END AS priority_group,
        ROUND((UNIX_TIMESTAMP(closed_at) - UNIX_TIMESTAMP(opened_at)) / 60, 1) AS minutes_opened_at_to_closed_at
    FROM tickets
    WHERE agent_id <= 8
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        priority,
        channel,
        priority_group,
        minutes_opened_at_to_closed_at,
        SIZE(COLLECT_SET(channel) OVER (PARTITION BY priority)) AS distinct_channel_in_priority,
        RANK() OVER (PARTITION BY agent_id ORDER BY opened_at DESC) AS opened_at_rank,
        RANK() OVER w_all_by_requester_id AS requester_id_rank,
        DENSE_RANK() OVER (ORDER BY requester_id) AS requester_id_dense_rank,
        NTILE(3) OVER (ORDER BY requester_id, id) AS requester_id_tercile,
        ROUND(requester_id / NULLIF(SUM(requester_id) OVER (PARTITION BY priority), 0), 4) AS requester_id_share_of_priority,
        ROW_NUMBER() OVER w_all_by_requester_id AS requester_id_seq,
        LAG(opened_at) OVER w_priority_group_agent_id_by_m AS prev_opened_at,
        MAX(requester_id) OVER (PARTITION BY agent_id ORDER BY closed_at DESC, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS requester_id_max_6_prior,
        ROUND(PERCENT_RANK() OVER w_all_by_requester_id_ties, 3) AS requester_id_percent_rank,
        NTH_VALUE(requester_id, 2) OVER (PARTITION BY agent_id ORDER BY opened_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_requester_id,
        FIRST_VALUE(requester_id) IGNORE NULLS OVER (PARTITION BY priority_group, agent_id ORDER BY minutes_opened_at_to_closed_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_requester_id
    FROM cleaned
    WINDOW w_all_by_requester_id AS (ORDER BY requester_id, id), w_priority_group_agent_id_by_m AS (PARTITION BY priority_group, agent_id ORDER BY minutes_opened_at_to_closed_at, id), w_all_by_requester_id_ties AS (ORDER BY requester_id)
),
top_per_group AS (
    -- Top 3 per group
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        priority,
        channel,
        priority_group,
        minutes_opened_at_to_closed_at,
        distinct_channel_in_priority,
        opened_at_rank,
        requester_id_rank,
        requester_id_tercile,
        requester_id_share_of_priority,
        requester_id_seq,
        prev_opened_at,
        requester_id_max_6_prior,
        requester_id_percent_rank,
        second_requester_id,
        first_known_requester_id
    FROM with_windows
    WHERE requester_id_dense_rank <= 3
),
metrics AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        COUNT(*) OVER (PARTITION BY MONTH(opened_at), requester_id_tercile) AS rows_in_opened_at_month_requester_id_tercile,
        LAST_VALUE(requester_id_tercile) OVER (PARTITION BY distinct_channel_in_priority, priority_group ORDER BY minutes_opened_at_to_closed_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_requester_id_tercile,
        ROW_NUMBER() OVER (PARTITION BY distinct_channel_in_priority, agent_id ORDER BY requester_id_percent_rank, id) AS rn,
        ROUND(CUME_DIST() OVER (PARTITION BY distinct_channel_in_priority, priority_group ORDER BY minutes_opened_at_to_closed_at), 3) AS minutes_opened_at_to_closed_at_cume_dist,
        LEAD(distinct_channel_in_priority) OVER w_all_by_distinct_channel_in_p AS next_distinct_channel_in_priority
    FROM top_per_group
    WINDOW w_all_by_distinct_channel_in_p AS (ORDER BY distinct_channel_in_priority, id)
),
grouped AS (
    -- Group-level totals
    SELECT
        requester_id_seq,
        COUNT(*) FILTER (WHERE priority <> 'P4') AS n_priority_p4,
        SUM(CASE WHEN priority = 'P1' THEN rows_in_opened_at_month_requester_id_tercile ELSE 0 END) AS rows_in_opened_at_month_requester_id_tercile_pri
    FROM metrics
    WHERE HOUR(prev_opened_at) BETWEEN 6 AND 10
    GROUP BY requester_id_seq
    HAVING MAX(requester_id_rank) > 4
),
with_trends AS (
    -- Running and moving aggregates
    SELECT
        requester_id_seq,
        n_priority_p4,
        ROUND(AVG(n_priority_p4) OVER (ORDER BY n_priority_p4, requester_id_seq ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING), 2) AS n_priority_p4_avg_3_prior,
        LEAD(requester_id_seq) OVER w_all_by_n_priority_p4 AS next_requester_id_seq
    FROM grouped
    WINDOW w_all_by_n_priority_p4 AS (ORDER BY n_priority_p4, requester_id_seq)
)
-- Final projection
SELECT DISTINCT next_requester_id_seq
FROM with_trends
WHERE n_priority_p4_avg_3_prior < 8.5
ORDER BY next_requester_id_seq DESC NULLS FIRST
