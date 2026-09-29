WITH cleaned_tickets AS (
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
        tags
    FROM tickets
    WHERE requester_id < 23
),
with_windows AS (
    -- Previous / next values per key
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        closed_at,
        channel,
        csat,
        ROW_NUMBER() OVER w_channel_by_opened_at AS opened_at_seq,
        ROUND(csat - AVG(csat) OVER (PARTITION BY channel), 2) AS csat_vs_channel_avg,
        COUNT(*) OVER (PARTITION BY channel) AS rows_in_channel,
        SIZE(COLLECT_SET(priority) OVER (PARTITION BY channel)) AS distinct_priority_in_channel,
        ROUND((UNIX_TIMESTAMP(closed_at) - UNIX_TIMESTAMP(LAG(closed_at) OVER w_all_by_requester_id)) / 60, 1) AS minutes_since_prev_closed_at,
        ROUND(AVG(csat) OVER w_channel, 2) AS channel_avg_csat,
        MIN(csat) OVER (PARTITION BY channel) AS channel_min_csat,
        SIZE(COLLECT_SET(channel) OVER (PARTITION BY csat)) AS distinct_channel_in_csat,
        csat - LAG(csat) OVER w_csat_priority_by_csat AS csat_change,
        RANK() OVER (PARTITION BY csat, priority ORDER BY csat DESC) AS csat_rank,
        SUM(requester_id) OVER (PARTITION BY channel ORDER BY opened_at NULLS LAST, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS requester_id_sum_3,
        ROUND(CUME_DIST() OVER (PARTITION BY csat, priority ORDER BY csat DESC), 3) AS csat_cume_dist
    FROM cleaned_tickets
    WINDOW w_channel_by_opened_at AS (PARTITION BY channel ORDER BY opened_at NULLS LAST, id), w_all_by_requester_id AS (ORDER BY requester_id DESC, id), w_channel AS (PARTITION BY channel), w_csat_priority_by_csat AS (PARTITION BY csat, priority ORDER BY csat DESC, id)
),
enriched AS (
    -- Derive helper columns
    SELECT
        *,
        SUBSTRING(TRIM(channel), 1, 2) AS channel_prefix,
        CASE WHEN channel IN ('email', 'web') THEN 'group_a' WHEN channel = 'phone' THEN 'group_b' ELSE 'other' END AS channel_group
    FROM with_windows
),
with_windows_2 AS (
    -- Window metrics over several partitions
    SELECT
        *,
        RANK() OVER (ORDER BY opened_at ASC, id) AS opened_at_rank,
        LEAD(csat_vs_channel_avg, 3) OVER w_agent_id_distinct_channel_in AS next3_csat_vs_channel_avg,
        MAX(rows_in_channel) OVER w_channel_group_opened_at_seq AS channel_group_opened_at_seq_max_rows_in_channel,
        SIZE(COLLECT_SET(agent_id) OVER w_channel_group_opened_at_seq) AS distinct_agent_id_in_channel_group_opened_at_seq
    FROM enriched
    WINDOW w_agent_id_distinct_channel_in AS (PARTITION BY agent_id, distinct_channel_in_csat ORDER BY opened_at NULLS LAST, id), w_channel_group_opened_at_seq AS (PARTITION BY channel_group, opened_at_seq)
)
SELECT
    id,
    requester_id,
    opened_at,
    closed_at,
    channel,
    csat,
    opened_at_seq,
    csat_vs_channel_avg,
    rows_in_channel,
    minutes_since_prev_closed_at,
    channel_avg_csat,
    channel_min_csat,
    csat_change,
    requester_id_sum_3,
    csat_cume_dist,
    channel_group,
    opened_at_rank,
    channel_group_opened_at_seq_max_rows_in_channel,
    distinct_agent_id_in_channel_group_opened_at_seq,
    LENGTH(TRIM(channel)) AS channel_len
FROM with_windows_2
ORDER BY csat_change DESC
