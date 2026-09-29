WITH recent_tickets AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        closed_at,
        priority,
        channel,
        csat,
        TO_DATE(closed_at) AS closed_at_date,
        LOWER(TRIM(priority)) AS priority_clean
    FROM tickets
    WHERE channel NOT IN ('phone', 'chat')
      AND HOUR(closed_at) BETWEEN 9 AND 14
),
sequenced AS (
    -- Running and moving aggregates
    SELECT
        id,
        agent_id,
        opened_at,
        closed_at,
        channel,
        closed_at_date,
        priority_clean,
        ROUND(csat - AVG(csat) OVER (PARTITION BY priority), 2) AS csat_vs_priority_avg,
        LAG(opened_at) OVER (PARTITION BY channel, DATE_TRUNC('MONTH', closed_at) ORDER BY closed_at ASC, id) AS prev_opened_at,
        COUNT(*) OVER (PARTITION BY priority) AS rows_in_priority,
        ROUND((UNIX_TIMESTAMP(closed_at) - UNIX_TIMESTAMP(LAG(closed_at) OVER (PARTITION BY channel, priority ORDER BY opened_at DESC, id))) / 60, 1) AS minutes_since_prev_closed_at,
        MAX(requester_id) OVER (PARTITION BY channel, DATE_TRUNC('MONTH', closed_at) ORDER BY closed_at ASC, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS requester_id_max_1_prior,
        ROUND(requester_id / NULLIF(SUM(requester_id) OVER (PARTITION BY priority), 0), 4) AS requester_id_share_of_priority,
        ROUND(CUME_DIST() OVER w_channel_priority_by_opened_a_2, 3) AS opened_at_cume_dist,
        LAST_VALUE(closed_at) OVER (PARTITION BY channel, priority ORDER BY opened_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_closed_at,
        MAX(csat) OVER w_priority AS priority_max_csat,
        COUNT(*) OVER (PARTITION BY agent_id ORDER BY closed_at_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count,
        NTILE(3) OVER (PARTITION BY channel, DATE_TRUNC('MONTH', closed_at) ORDER BY closed_at ASC, id) AS closed_at_tercile,
        RANK() OVER (PARTITION BY channel, DATE_TRUNC('MONTH', closed_at) ORDER BY closed_at ASC) AS closed_at_rank
    FROM recent_tickets
    WINDOW w_channel_priority_by_opened_a_2 AS (PARTITION BY channel, priority ORDER BY opened_at DESC), w_priority AS (PARTITION BY priority)
),
top_per_group AS (
    -- Top 3 per group
    SELECT
        id,
        agent_id,
        opened_at,
        closed_at,
        channel,
        closed_at_date,
        priority_clean,
        csat_vs_priority_avg,
        prev_opened_at,
        rows_in_priority,
        minutes_since_prev_closed_at,
        requester_id_max_1_prior,
        requester_id_share_of_priority,
        opened_at_cume_dist,
        last_closed_at,
        priority_max_csat,
        running_count,
        closed_at_tercile
    FROM sequenced
    WHERE closed_at_rank <= 3
      AND last_closed_at >= TIMESTAMP '2025-01-15 08:00:00'
),
grouped AS (
    -- Group-level totals
    SELECT
        TO_DATE(prev_opened_at) AS prev_opened_at_day,
        ROUND(100 * COUNT(CASE WHEN channel IS NOT NULL THEN 1 END) / COUNT(*), 1) AS pct_channel_is_not_null,
        ARRAY_SORT(COLLECT_SET(channel)) AS channel_list,
        SUM(CASE WHEN channel = 'phone' THEN 1 ELSE 0 END) AS n_channel_phone,
        SUM(CASE WHEN channel = 'chat' THEN 1 ELSE 0 END) AS n_channel_chat,
        ROUND(SUM(csat_vs_priority_avg) / NULLIF(SUM(priority_max_csat), 0), 3) AS csat_vs_priority_avg_to_priority_max_csat_ratio,
        MIN(opened_at_cume_dist) AS min_opened_at_cume_dist
    FROM top_per_group
    GROUP BY TO_DATE(prev_opened_at)
    HAVING MAX(csat_vs_priority_avg) > -3.5
),
with_trends AS (
    -- Rank and compare rows within groups
    SELECT
        prev_opened_at_day,
        pct_channel_is_not_null,
        channel_list,
        n_channel_phone,
        n_channel_chat,
        csat_vs_priority_avg_to_priority_max_csat_ratio,
        min_opened_at_cume_dist,
        SUM(n_channel_phone) OVER (PARTITION BY n_channel_chat ORDER BY min_opened_at_cume_dist, prev_opened_at_day ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_n_channel_phone,
        SUM(min_opened_at_cume_dist) OVER (PARTITION BY n_channel_chat ORDER BY min_opened_at_cume_dist, prev_opened_at_day ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_min_opened_at_cume_dist,
        ROUND(PERCENT_RANK() OVER (PARTITION BY n_channel_chat ORDER BY min_opened_at_cume_dist), 3) AS min_opened_at_cume_dist_percent_rank,
        NTH_VALUE(min_opened_at_cume_dist, 2) OVER (PARTITION BY n_channel_chat ORDER BY min_opened_at_cume_dist, prev_opened_at_day ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_min_opened_at_cume_dist
    FROM grouped
)
SELECT
    prev_opened_at_day,
    pct_channel_is_not_null,
    channel_list,
    n_channel_phone,
    n_channel_chat,
    csat_vs_priority_avg_to_priority_max_csat_ratio,
    running_n_channel_phone,
    running_min_opened_at_cume_dist,
    min_opened_at_cume_dist_percent_rank,
    second_min_opened_at_cume_dist,
    DATEDIFF(DATE '2025-06-30', prev_opened_at_day) AS days_since_prev_opened_at_day
FROM with_trends
ORDER BY prev_opened_at_day DESC
