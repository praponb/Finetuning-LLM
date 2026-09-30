WITH cleaned AS (
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
        tags,
        LENGTH(TRIM(priority)) AS priority_len,
        COALESCE(channel, 'UNKNOWN') AS channel_or_unknown
    FROM tickets
    WHERE ARRAY_CONTAINS(tags, 'refund')
),
metrics AS (
    -- Window metrics over several partitions
    SELECT
        id,
        priority,
        priority_len,
        channel_or_unknown,
        ROUND(csat - AVG(csat) OVER w_agent_id_priority, 2) AS csat_vs_agent_id_priority_avg,
        ROUND(priority_len / NULLIF(SUM(priority_len) OVER w_closed_at_month_priority, 0), 4) AS priority_len_share_of_closed_at_month_priority,
        SUM(priority_len) OVER w_closed_at_month_priority AS closed_at_month_priority_sum_priority_len,
        RANK() OVER (PARTITION BY agent_id ORDER BY opened_at DESC, id) AS opened_at_rank,
        FIRST_VALUE(csat) OVER (ORDER BY opened_at ASC NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_csat,
        ROW_NUMBER() OVER (PARTITION BY agent_id ORDER BY closed_at DESC, id) AS seq_no,
        SUM(priority_len) OVER (PARTITION BY priority_len, agent_id ORDER BY csat DESC, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS priority_len_sum_1_prior,
        LEAD(closed_at) OVER (PARTITION BY priority_len, agent_id ORDER BY csat DESC, id) AS next_closed_at,
        SUM(priority_len) OVER (ORDER BY opened_at ASC NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_priority_len,
        SIZE(COLLECT_SET(channel_or_unknown) OVER (PARTITION BY agent_id, priority)) AS distinct_channel_or_unknown_in_agent_id_priority,
        DENSE_RANK() OVER w_priority_len_by_opened_at_ti AS opened_at_dense_rank,
        ROUND(AVG(csat) OVER (PARTITION BY agent_id ORDER BY closed_at DESC, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING), 2) AS csat_avg_6_prior,
        ROUND(CUME_DIST() OVER w_priority_len_by_opened_at, 3) AS opened_at_cume_dist,
        NTH_VALUE(requester_id, 2) OVER (PARTITION BY priority_len, agent_id ORDER BY csat DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_requester_id
    FROM cleaned
    WINDOW w_agent_id_priority AS (PARTITION BY agent_id, priority), w_closed_at_month_priority AS (PARTITION BY MONTH(closed_at), priority), w_priority_len_by_opened_at_ti AS (PARTITION BY priority_len ORDER BY opened_at), w_priority_len_by_opened_at AS (PARTITION BY priority_len ORDER BY opened_at, id)
)
-- Final projection
SELECT
    id,
    priority,
    priority_len,
    channel_or_unknown,
    seq_no,
    priority_len_sum_1_prior,
    next_closed_at,
    csat_avg_6_prior,
    opened_at_cume_dist,
    second_requester_id
FROM metrics
ORDER BY id DESC NULLS FIRST, priority_len DESC
