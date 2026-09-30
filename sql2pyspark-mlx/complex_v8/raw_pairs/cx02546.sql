WITH filtered AS (
    -- Scope the tickets rows
    SELECT
        id,
        requester_id,
        agent_id,
        priority,
        channel,
        DATE_TRUNC('WEEK', closed_at) AS closed_at_week
    FROM tickets
    WHERE requester_id <= 23
),
combined_details AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.requester_id,
        s.agent_id,
        s.priority,
        s.channel,
        s.closed_at_week,
        su.hired_on AS agent_hired_on,
        su.agent_name
    FROM filtered s
    JOIN support_agents su ON su.id = s.agent_id
),
with_windows AS (
    -- Window metrics over several partitions
    SELECT
        *,
        RANK() OVER (PARTITION BY channel, priority ORDER BY closed_at_week) AS closed_at_week_rank,
        ROUND(requester_id / NULLIF(SUM(requester_id) OVER (PARTITION BY priority), 0), 4) AS requester_id_share_of_priority,
        ROUND(requester_id - AVG(requester_id) OVER (PARTITION BY priority), 2) AS requester_id_vs_priority_avg,
        LEAD(requester_id, 3, 0) OVER (ORDER BY requester_id DESC, id) AS next3_requester_id,
        SIZE(COLLECT_SET(agent_id) OVER w_priority) AS distinct_agent_id_in_priority,
        ROUND(AVG(requester_id) OVER (PARTITION BY priority), 2) AS priority_avg_requester_id,
        DENSE_RANK() OVER w_all_by_requester_id AS requester_id_dense_rank,
        DATEDIFF(agent_hired_on, LAG(agent_hired_on) OVER (PARTITION BY channel, priority ORDER BY closed_at_week, id)) AS days_since_prev_agent_hired_on,
        LAG(requester_id) OVER w_priority_agent_id_by_agent_h AS prev_requester_id,
        NTH_VALUE(requester_id, 2) OVER (PARTITION BY channel, priority ORDER BY closed_at_week, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_requester_id,
        NTILE(3) OVER (ORDER BY requester_id DESC, id) AS requester_id_tercile,
        LEAD(closed_at_week, 2) OVER w_all_by_requester_id AS next2_closed_at_week,
        LEAD(closed_at_week, 3) OVER (ORDER BY requester_id DESC, id) AS next3_closed_at_week
    FROM combined_details
    WINDOW w_priority AS (PARTITION BY priority), w_all_by_requester_id AS (ORDER BY requester_id DESC, id), w_priority_agent_id_by_agent_h AS (PARTITION BY priority, agent_id ORDER BY agent_hired_on ASC, id)
),
sequenced AS (
    -- Running and moving aggregates
    SELECT
        id,
        priority,
        channel,
        closed_at_week,
        agent_hired_on,
        agent_name,
        closed_at_week_rank,
        next3_requester_id,
        days_since_prev_agent_hired_on,
        prev_requester_id,
        requester_id_tercile,
        next3_closed_at_week,
        SIZE(COLLECT_SET(agent_id) OVER (PARTITION BY DATE_TRUNC('MONTH', agent_hired_on))) AS distinct_agent_id_in_agent_hired_on_month,
        DENSE_RANK() OVER (PARTITION BY requester_id_tercile, channel ORDER BY distinct_agent_id_in_priority) AS distinct_agent_id_in_priority_dense_rank,
        ROW_NUMBER() OVER w_all_by_next3_requester_id AS rn,
        LEAD(closed_at_week_rank, 3) OVER w_requester_id_tercile_channel_2 AS next3_closed_at_week_rank,
        ROUND(CUME_DIST() OVER (PARTITION BY requester_id_dense_rank ORDER BY requester_id_vs_priority_avg DESC), 3) AS requester_id_vs_priority_avg_cume_dist,
        ROUND(AVG(requester_id_tercile) OVER (PARTITION BY priority, requester_id_dense_rank ORDER BY distinct_agent_id_in_priority DESC NULLS FIRST, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2) AS requester_id_tercile_avg_3,
        COUNT(*) OVER (PARTITION BY requester_id_dense_rank, requester_id_tercile) AS rows_in_requester_id_dense_rank_requester_id_ter,
        LAST_VALUE(requester_id_dense_rank) IGNORE NULLS OVER (ORDER BY next3_requester_id, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_requester_id_dense_rank
    FROM with_windows
    WINDOW w_all_by_next3_requester_id AS (ORDER BY next3_requester_id, id), w_requester_id_tercile_channel_2 AS (PARTITION BY requester_id_tercile, channel ORDER BY distinct_agent_id_in_priority, id)
)
-- Final projection
SELECT
    id,
    priority,
    channel,
    agent_hired_on,
    agent_name,
    closed_at_week_rank,
    days_since_prev_agent_hired_on,
    prev_requester_id,
    requester_id_tercile,
    next3_closed_at_week,
    distinct_agent_id_in_agent_hired_on_month,
    distinct_agent_id_in_priority_dense_rank,
    rn,
    requester_id_vs_priority_avg_cume_dist,
    requester_id_tercile_avg_3,
    rows_in_requester_id_dense_rank_requester_id_ter,
    last_known_requester_id_dense_rank,
    COALESCE(channel, 'UNKNOWN') AS channel_or_unknown,
    CASE WHEN requester_id_tercile >= 7 THEN 'large' WHEN requester_id_tercile >= 4 THEN 'medium' ELSE 'small' END AS requester_id_tercile_tier
FROM sequenced
