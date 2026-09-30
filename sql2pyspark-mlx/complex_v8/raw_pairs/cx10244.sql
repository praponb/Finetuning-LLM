WITH filtered_tickets AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        requester_id,
        agent_id,
        closed_at,
        priority,
        channel,
        csat,
        tags,
        CASE WHEN channel IN ('web', 'phone') THEN 'group_a' WHEN channel = 'email' THEN 'group_b' ELSE 'other' END AS channel_group,
        COALESCE(csat, 0) AS csat_filled
    FROM tickets
    WHERE (agent_id IS NOT NULL OR requester_id < 17)
),
metrics AS (
    -- Previous / next values per key
    SELECT
        id,
        requester_id,
        agent_id,
        csat,
        tags,
        MAX(csat_filled) OVER (PARTITION BY priority ORDER BY closed_at DESC, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS csat_filled_max_2_prior,
        SIZE(COLLECT_SET(channel_group) OVER (PARTITION BY TO_DATE(closed_at), csat)) AS distinct_channel_group_in_closed_at_to_date_csat,
        COUNT(*) OVER w_csat_filled_priority AS rows_in_csat_filled_priority,
        ROUND(csat_filled - AVG(csat_filled) OVER (PARTITION BY csat_filled, priority), 2) AS csat_filled_vs_csat_filled_priority_avg,
        ROUND(PERCENT_RANK() OVER (ORDER BY csat DESC, id), 3) AS csat_percent_rank,
        ROUND((UNIX_TIMESTAMP(closed_at) - UNIX_TIMESTAMP(LAG(closed_at) OVER (PARTITION BY channel ORDER BY csat_filled, id))) / 60, 1) AS minutes_since_prev_closed_at,
        COUNT(*) OVER (PARTITION BY DATE_TRUNC('MONTH', closed_at), priority ORDER BY requester_id, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count,
        DENSE_RANK() OVER (PARTITION BY priority ORDER BY closed_at DESC, id) AS closed_at_dense_rank,
        COUNT(*) OVER (PARTITION BY TO_DATE(closed_at), csat) AS rows_in_closed_at_to_date_csat,
        COUNT(*) OVER (ORDER BY csat DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count_2,
        RANK() OVER w_channel_by_csat_filled_ties AS csat_filled_rank,
        ROUND(csat / NULLIF(SUM(csat) OVER (PARTITION BY channel), 0), 4) AS csat_share_of_channel,
        MIN(csat_filled) OVER (PARTITION BY channel, MONTH(closed_at) ORDER BY csat_filled DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_csat_filled_so_far
    FROM filtered_tickets
    WINDOW w_csat_filled_priority AS (PARTITION BY csat_filled, priority), w_channel_by_csat_filled_ties AS (PARTITION BY channel ORDER BY csat_filled)
),
grouped AS (
    -- Summarise per group
    SELECT
        closed_at_dense_rank,
        csat,
        csat_filled_rank,
        COUNT(*) AS cnt,
        ROUND(AVG(csat), 2) AS avg_csat,
        SUM(rows_in_csat_filled_priority) AS total_rows_in_csat_filled_priority
    FROM metrics
    WHERE minutes_since_prev_closed_at < 3250.0
    GROUP BY closed_at_dense_rank, csat, csat_filled_rank
)
-- Final projection
SELECT
    closed_at_dense_rank,
    csat,
    cnt,
    total_rows_in_csat_filled_priority,
    COALESCE(avg_csat, 0.0) AS avg_csat_filled,
    ROUND(closed_at_dense_rank / NULLIF(csat, 0), 3) AS closed_at_dense_rank_per_csat
FROM grouped
ORDER BY cnt DESC
