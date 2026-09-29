WITH recent_tickets AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        closed_at,
        tags,
        DATE_FORMAT(closed_at, 'yyyy-MM') AS closed_at_label,
        LENGTH(TRIM(channel)) AS channel_len
    FROM tickets
    WHERE channel NOT IN ('email', 'web')
),
with_windows AS (
    -- Previous / next values per key
    SELECT
        id,
        agent_id,
        opened_at,
        closed_at,
        tags,
        channel_len,
        SIZE(COLLECT_SET(agent_id) OVER (PARTITION BY YEAR(closed_at), channel_len)) AS distinct_agent_id_in_closed_at_year_channel_len,
        DENSE_RANK() OVER (ORDER BY channel_len) AS channel_len_dense_rank,
        NTILE(5) OVER (ORDER BY channel_len, id) AS channel_len_quintile,
        COUNT(*) OVER (PARTITION BY YEAR(closed_at), channel_len) AS rows_in_closed_at_year_channel_len,
        MIN(requester_id) OVER w_closed_at_year_channel_len AS closed_at_year_channel_len_min_requester_id,
        ROUND(channel_len - AVG(channel_len) OVER (PARTITION BY agent_id), 2) AS channel_len_vs_agent_id_avg,
        ROUND(requester_id - AVG(requester_id) OVER (PARTITION BY agent_id), 2) AS requester_id_vs_agent_id_avg,
        MIN(channel_len) OVER w_agent_id AS agent_id_min_channel_len,
        ROUND(channel_len - AVG(channel_len) OVER (PARTITION BY YEAR(closed_at), channel_len), 2) AS channel_len_vs_closed_at_year_channel_len_avg,
        ROUND(requester_id / NULLIF(SUM(requester_id) OVER (PARTITION BY DATE_TRUNC('MONTH', opened_at)), 0), 4) AS requester_id_share_of_opened_at_month,
        DENSE_RANK() OVER w_closed_at_to_date_by_closed AS closed_at_dense_rank,
        MAX(requester_id) OVER (ORDER BY closed_at DESC NULLS FIRST, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS requester_id_max_3_prior,
        SUM(requester_id) OVER (PARTITION BY DATE_TRUNC('MONTH', opened_at)) AS opened_at_month_sum_requester_id,
        COUNT(*) OVER (PARTITION BY DATE_TRUNC('MONTH', opened_at)) AS rows_in_opened_at_month,
        ROUND(requester_id - AVG(requester_id) OVER (PARTITION BY YEAR(closed_at), channel_len), 2) AS requester_id_vs_closed_at_year_channel_len_avg
    FROM recent_tickets
    WINDOW w_closed_at_year_channel_len AS (PARTITION BY YEAR(closed_at), channel_len), w_agent_id AS (PARTITION BY agent_id), w_closed_at_to_date_by_closed AS (PARTITION BY TO_DATE(closed_at) ORDER BY closed_at DESC, id)
),
totals AS (
    -- Summarise per group
    SELECT
        channel_len_quintile,
        closed_at_dense_rank,
        channel_len,
        SUM(rows_in_opened_at_month) AS total_rows_in_opened_at_month,
        ROUND(SUM(channel_len_vs_closed_at_year_channel_len_avg) / NULLIF(SUM(requester_id_max_3_prior), 0), 3) AS channel_len_vs_closed_at_year_channel_len_avg_to,
        MIN(opened_at) AS first_opened_at,
        COUNT(*) AS row_count,
        MAX(opened_at) AS last_opened_at,
        MIN(closed_at) AS first_closed_at
    FROM with_windows
    GROUP BY channel_len_quintile, closed_at_dense_rank, channel_len
),
with_trends AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROUND(AVG(row_count) OVER (PARTITION BY row_count, channel_len_quintile ORDER BY total_rows_in_opened_at_month DESC, channel_len_quintile, closed_at_dense_rank, channel_len ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS row_count_avg_7,
        LEAD(closed_at_dense_rank, 3) OVER (PARTITION BY channel_len_quintile ORDER BY channel_len_vs_closed_at_year_channel_len_avg_to DESC, channel_len_quintile, closed_at_dense_rank, channel_len) AS next3_closed_at_dense_rank
    FROM totals
)
SELECT
    channel_len_quintile,
    closed_at_dense_rank,
    channel_len,
    total_rows_in_opened_at_month,
    first_opened_at,
    row_count,
    row_count_avg_7,
    next3_closed_at_dense_rank,
    DATE_TRUNC('MONTH', first_closed_at) AS first_closed_at_month
FROM with_trends
ORDER BY total_rows_in_opened_at_month DESC, closed_at_dense_rank NULLS LAST
