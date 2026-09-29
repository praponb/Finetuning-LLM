WITH base AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        agent_id,
        closed_at,
        channel,
        tags,
        DATE_FORMAT(opened_at, 'yyyy-MM') AS opened_at_label
    FROM tickets
    WHERE MONTH(closed_at) IN (1, 2)
      AND opened_at >= TIMESTAMP '2025-01-13 12:00:00'
),
with_trends AS (
    -- Previous / next values per key
    SELECT
        id,
        agent_id,
        closed_at,
        channel,
        tags,
        LAST_VALUE(closed_at) OVER (ORDER BY closed_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_closed_at,
        ROUND(PERCENT_RANK() OVER (ORDER BY closed_at DESC), 3) AS closed_at_percent_rank,
        ROW_NUMBER() OVER w_all_by_closed_at AS closed_at_seq,
        NTH_VALUE(closed_at, 2) OVER (ORDER BY closed_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_closed_at,
        ROUND(PERCENT_RANK() OVER (ORDER BY closed_at), 3) AS closed_at_percent_rank_2,
        NTH_VALUE(closed_at, 2) OVER (ORDER BY closed_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_closed_at_3,
        RANK() OVER (PARTITION BY channel, agent_id ORDER BY closed_at) AS closed_at_rank,
        ROUND((UNIX_TIMESTAMP(closed_at) - UNIX_TIMESTAMP(LAG(closed_at) OVER (ORDER BY closed_at DESC, id))) / 60, 1) AS minutes_since_prev_closed_at,
        DENSE_RANK() OVER w_all_by_closed_at AS closed_at_dense_rank,
        ROUND(PERCENT_RANK() OVER (ORDER BY closed_at, id), 3) AS closed_at_percent_rank_3
    FROM base
    WINDOW w_all_by_closed_at AS (ORDER BY closed_at, id)
),
sequenced AS (
    -- Running and moving aggregates
    SELECT
        id,
        closed_at,
        channel,
        tags,
        closed_at_percent_rank,
        closed_at_seq,
        second_closed_at,
        closed_at_percent_rank_2,
        second_closed_at_3,
        closed_at_rank,
        minutes_since_prev_closed_at,
        closed_at_dense_rank,
        closed_at_percent_rank_3,
        COUNT(*) OVER (PARTITION BY channel, closed_at_dense_rank) AS rows_in_channel_closed_at_dense_rank,
        DENSE_RANK() OVER w_all_by_second_closed_at AS second_closed_at_dense_rank,
        SIZE(COLLECT_SET(agent_id) OVER (PARTITION BY channel, closed_at_dense_rank)) AS distinct_agent_id_in_channel_closed_at_dense_ran,
        MIN(closed_at_rank) OVER (PARTITION BY channel, closed_at_dense_rank) AS channel_closed_at_dense_rank_min_closed_at_rank,
        MIN(closed_at_dense_rank) OVER (PARTITION BY channel, closed_at_dense_rank) AS channel_closed_at_dense_rank_min_closed_at_dense,
        NTH_VALUE(closed_at_percent_rank_2, 2) OVER (PARTITION BY DATE_TRUNC('MONTH', closed_at) ORDER BY closed_at_percent_rank_3 DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_closed_at_percent_rank_2,
        minutes_since_prev_closed_at - LAG(minutes_since_prev_closed_at) OVER (PARTITION BY channel ORDER BY closed_at_seq NULLS LAST, id) AS minutes_since_prev_closed_at_change,
        MIN(closed_at_percent_rank_3) OVER w_closed_at_seq AS closed_at_seq_min_closed_at_percent_rank_3
    FROM with_trends
    WINDOW w_all_by_second_closed_at AS (ORDER BY second_closed_at DESC, id), w_closed_at_seq AS (PARTITION BY closed_at_seq)
),
summary AS (
    -- Summarise per group
    SELECT
        DATE_TRUNC('MONTH', second_closed_at) AS second_closed_at_month,
        channel,
        ROUND(AVG(second_closed_at_dense_rank), 2) AS avg_second_closed_at_dense_rank,
        SUM(CASE WHEN channel = 'chat' THEN 1 ELSE 0 END) AS n_channel_chat,
        COUNT(*) AS row_count,
        ROUND(SUM(closed_at_seq_min_closed_at_percent_rank_3), 2) AS total_closed_at_seq_min_closed_at_percent_rank_3,
        ARRAY_SORT(COLLECT_SET(channel)) AS channel_list
    FROM sequenced
    GROUP BY DATE_TRUNC('MONTH', second_closed_at), channel
)
-- Final projection
SELECT
    second_closed_at_month,
    channel,
    avg_second_closed_at_dense_rank,
    n_channel_chat,
    row_count,
    total_closed_at_seq_min_closed_at_percent_rank_3,
    DATE_FORMAT(second_closed_at_month, 'yyyy-MM-dd HH:00') AS second_closed_at_month_label,
    LOWER(REGEXP_REPLACE(TRIM(channel), ' ', '_')) AS channel_slug
FROM summary
ORDER BY channel_slug DESC, row_count DESC
