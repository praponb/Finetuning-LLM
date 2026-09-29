WITH scoped_tickets AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        closed_at,
        priority,
        channel,
        LENGTH(TRIM(priority)) AS priority_len
    FROM tickets
    WHERE SIZE(tags) >= 1
),
sequenced AS (
    -- Window metrics over several partitions
    SELECT
        id,
        agent_id,
        opened_at,
        closed_at,
        priority_len,
        MAX(requester_id) OVER (PARTITION BY channel) AS channel_max_requester_id,
        ROUND(PERCENT_RANK() OVER (PARTITION BY YEAR(opened_at) ORDER BY opened_at DESC, id), 3) AS opened_at_percent_rank,
        ROUND(PERCENT_RANK() OVER (PARTITION BY priority_len, priority ORDER BY closed_at ASC), 3) AS closed_at_percent_rank,
        FIRST_VALUE(opened_at) IGNORE NULLS OVER (PARTITION BY agent_id, channel ORDER BY opened_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_opened_at,
        ROUND(priority_len - AVG(priority_len) OVER (PARTITION BY channel), 2) AS priority_len_vs_channel_avg,
        LAG(priority_len) OVER (PARTITION BY YEAR(closed_at), priority_len ORDER BY closed_at, id) AS prev_priority_len,
        ROUND(AVG(priority_len) OVER (PARTITION BY agent_id, channel ORDER BY opened_at DESC, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING), 2) AS priority_len_avg_2_prior,
        ROUND(AVG(requester_id) OVER (PARTITION BY channel), 2) AS channel_avg_requester_id
    FROM scoped_tickets
),
enriched AS (
    -- Derive helper columns
    SELECT
        *,
        DATE_FORMAT(opened_at, 'yyyy-MM') AS opened_at_label,
        FLOOR(channel_avg_requester_id / 5) * 5 AS channel_avg_requester_id_bucket,
        DATE_FORMAT(first_known_opened_at, 'yyyy-MM-dd HH:00') AS first_known_opened_at_label
    FROM sequenced
),
rollup_base AS (
    -- Aggregate with conditional metrics
    SELECT
        agent_id,
        priority_len,
        prev_priority_len,
        COUNT(*) AS row_count,
        MIN(channel_max_requester_id) AS min_channel_max_requester_id,
        COUNT(DISTINCT agent_id) AS distinct_agent_ids,
        MAX(opened_at) AS last_opened_at,
        MIN(opened_at) AS first_opened_at
    FROM enriched
    GROUP BY agent_id, priority_len, prev_priority_len
),
with_windows AS (
    -- Previous / next values per key
    SELECT
        *,
        COUNT(*) OVER (PARTITION BY priority_len, TO_DATE(last_opened_at)) AS rows_in_priority_len_last_opened_at_to_date,
        prev_priority_len - LAG(prev_priority_len) OVER w_all_by_row_count AS prev_priority_len_change,
        FIRST_VALUE(row_count) IGNORE NULLS OVER (ORDER BY row_count NULLS LAST, agent_id, priority_len, prev_priority_len ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_row_count
    FROM rollup_base
    WINDOW w_all_by_row_count AS (ORDER BY row_count NULLS LAST, agent_id, priority_len, prev_priority_len)
)
SELECT
    agent_id,
    priority_len,
    row_count,
    min_channel_max_requester_id,
    distinct_agent_ids,
    last_opened_at,
    prev_priority_len_change,
    first_known_row_count,
    COALESCE(first_known_row_count, 0) AS first_known_row_count_filled,
    DATE_FORMAT(first_opened_at, 'MMM yyyy') AS first_opened_at_label
FROM with_windows
WHERE agent_id <= 6
ORDER BY priority_len DESC NULLS FIRST, first_known_row_count_filled
