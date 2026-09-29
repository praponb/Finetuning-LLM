WITH recent AS (
    -- Scope the tickets rows
    SELECT
        id,
        requester_id,
        agent_id,
        closed_at,
        priority,
        channel,
        tags,
        DATEDIFF(DATE '2025-12-31', opened_at) AS days_since_opened_at,
        SUBSTRING(TRIM(channel), 1, 2) AS channel_prefix
    FROM tickets
    WHERE TO_DATE(opened_at) < DATE '2025-03-14'
      AND closed_at < TIMESTAMP '2025-03-01 12:00:00'
),
with_details AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.requester_id,
        s.agent_id,
        s.priority,
        s.channel,
        s.tags,
        s.days_since_opened_at,
        su.agent_name
    FROM recent s
    JOIN support_agents su ON su.id = s.agent_id
    WHERE LENGTH(TRIM(su.agent_name)) >= 8
),
rollup_base AS (
    -- Group-level totals
    SELECT
        channel,
        ROUND(100 * COUNT(CASE WHEN UPPER(TRIM(agent_name)) LIKE 'K%' THEN 1 END) / COUNT(*), 1) AS pct_upper_trim_agent_name_like_k,
        ROUND(STDDEV(days_since_opened_at), 2) AS stddev_days_since_opened_at,
        COUNT(*) AS row_count,
        ARRAY_SORT(COLLECT_SET(agent_name)) AS agent_name_list,
        COUNT(DISTINCT priority) AS distinct_prioritys
    FROM with_details
    GROUP BY channel
),
ranked AS (
    -- Previous / next values per key
    SELECT
        channel,
        pct_upper_trim_agent_name_like_k,
        stddev_days_since_opened_at,
        row_count,
        agent_name_list,
        distinct_prioritys,
        COUNT(*) OVER (PARTITION BY channel, row_count) AS rows_in_channel_row_count,
        LEAD(stddev_days_since_opened_at, 1, 0) OVER (ORDER BY stddev_days_since_opened_at NULLS LAST, channel) AS next_stddev_days_since_opened_at,
        ROW_NUMBER() OVER (PARTITION BY distinct_prioritys ORDER BY distinct_prioritys DESC, channel) AS seq_no,
        ROUND(CUME_DIST() OVER (PARTITION BY distinct_prioritys ORDER BY distinct_prioritys DESC), 3) AS distinct_prioritys_cume_dist,
        ROUND(CUME_DIST() OVER w_all_by_stddev_days_since_ope_2, 3) AS stddev_days_since_opened_at_cume_dist
    FROM rollup_base
    WINDOW w_all_by_stddev_days_since_ope_2 AS (ORDER BY stddev_days_since_opened_at NULLS LAST)
),
sequenced AS (
    -- Rank and compare rows within groups
    SELECT
        channel,
        pct_upper_trim_agent_name_like_k,
        row_count,
        agent_name_list,
        distinct_prioritys,
        rows_in_channel_row_count,
        next_stddev_days_since_opened_at,
        distinct_prioritys_cume_dist,
        stddev_days_since_opened_at_cume_dist,
        ROUND(AVG(seq_no) OVER (PARTITION BY channel), 2) AS channel_avg_seq_no,
        RANK() OVER (PARTITION BY row_count, seq_no ORDER BY next_stddev_days_since_opened_at ASC, channel) AS next_stddev_days_since_opened_at_rank,
        DENSE_RANK() OVER (ORDER BY next_stddev_days_since_opened_at DESC) AS next_stddev_days_since_opened_at_dense_rank,
        DENSE_RANK() OVER (PARTITION BY row_count, seq_no ORDER BY next_stddev_days_since_opened_at ASC, channel) AS next_stddev_days_since_opened_at_dense_rank_2,
        ROUND(distinct_prioritys / NULLIF(SUM(distinct_prioritys) OVER w_channel, 0), 4) AS distinct_prioritys_share_of_channel
    FROM ranked
    WINDOW w_channel AS (PARTITION BY channel)
)
SELECT
    next_stddev_days_since_opened_at_dense_rank,
    next_stddev_days_since_opened_at_dense_rank_2,
    distinct_prioritys,
    SUM(row_count) AS total_row_count,
    SUM(rows_in_channel_row_count) AS total_rows_in_channel_row_count
FROM sequenced
GROUP BY CUBE(next_stddev_days_since_opened_at_dense_rank, next_stddev_days_since_opened_at_dense_rank_2, distinct_prioritys)
ORDER BY next_stddev_days_since_opened_at_dense_rank NULLS LAST, next_stddev_days_since_opened_at_dense_rank_2 NULLS LAST, distinct_prioritys NULLS LAST
