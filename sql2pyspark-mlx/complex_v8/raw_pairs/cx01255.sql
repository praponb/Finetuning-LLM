WITH prepared AS (
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
        SUBSTRING(TRIM(priority), 1, 3) AS priority_prefix
    FROM tickets
    WHERE opened_at >= TIMESTAMP '2025-01-31 08:00:00'
      AND closed_at < TIMESTAMP '2025-02-26 16:00:00'
),
with_trends AS (
    -- Previous / next values per key
    SELECT
        *,
        LEAD(closed_at, 2) OVER w_channel_priority_by_requeste AS next2_closed_at,
        COUNT(*) OVER (PARTITION BY MONTH(opened_at)) AS rows_in_opened_at_month,
        ROUND(PERCENT_RANK() OVER w_all_by_closed_at, 3) AS closed_at_percent_rank,
        ROUND(AVG(requester_id) OVER (ORDER BY opened_at, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW), 2) AS requester_id_avg_4,
        DENSE_RANK() OVER (ORDER BY closed_at DESC) AS closed_at_dense_rank,
        SUM(requester_id) OVER (PARTITION BY MONTH(opened_at)) AS opened_at_month_sum_requester_id,
        FIRST_VALUE(closed_at) OVER (PARTITION BY channel, priority ORDER BY requester_id DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_closed_at,
        ROUND(requester_id - AVG(requester_id) OVER (PARTITION BY DATE_TRUNC('MONTH', opened_at)), 2) AS requester_id_vs_opened_at_month_avg,
        requester_id - LAG(requester_id) OVER (ORDER BY closed_at DESC, id) AS requester_id_change
    FROM prepared
    WINDOW w_channel_priority_by_requeste AS (PARTITION BY channel, priority ORDER BY requester_id DESC NULLS FIRST, id), w_all_by_closed_at AS (ORDER BY closed_at DESC, id)
),
top_rows AS (
    -- First row per group
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        closed_at,
        priority,
        channel,
        tags,
        priority_prefix,
        next2_closed_at,
        rows_in_opened_at_month,
        closed_at_percent_rank,
        requester_id_avg_4,
        opened_at_month_sum_requester_id,
        first_closed_at,
        requester_id_vs_opened_at_month_avg,
        requester_id_change
    FROM with_trends
    WHERE closed_at_dense_rank = 1
      AND requester_id_change <= 12
),
windowed AS (
    -- Previous / next values per key
    SELECT
        id,
        agent_id,
        priority,
        channel,
        tags,
        priority_prefix,
        rows_in_opened_at_month,
        requester_id_avg_4,
        LEAD(opened_at) OVER w_channel_opened_at_month_by_o AS next_opened_at,
        DENSE_RANK() OVER (PARTITION BY channel, DATE_TRUNC('MONTH', opened_at) ORDER BY opened_at DESC) AS opened_at_dense_rank,
        DENSE_RANK() OVER w_channel_opened_at_month_by_o AS opened_at_dense_rank_2,
        LAST_VALUE(closed_at_percent_rank) IGNORE NULLS OVER (PARTITION BY channel, DATE_TRUNC('MONTH', opened_at) ORDER BY opened_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_closed_at_percent_rank,
        FIRST_VALUE(closed_at) IGNORE NULLS OVER (PARTITION BY channel, DATE_TRUNC('MONTH', opened_at) ORDER BY opened_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_closed_at,
        MAX(requester_id_change) OVER (ORDER BY requester_id, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS requester_id_change_max_7
    FROM top_rows
    WINDOW w_channel_opened_at_month_by_o AS (PARTITION BY channel, DATE_TRUNC('MONTH', opened_at) ORDER BY opened_at DESC, id)
)
SELECT
    id,
    channel,
    rows_in_opened_at_month,
    requester_id_avg_4,
    next_opened_at,
    opened_at_dense_rank,
    opened_at_dense_rank_2,
    last_known_closed_at_percent_rank,
    first_known_closed_at,
    ROUND((UNIX_TIMESTAMP(next_opened_at) - UNIX_TIMESTAMP(first_known_closed_at)) / 60, 1) AS minutes_first_known_closed_at_to_next_opened_at,
    DATE_TRUNC('QUARTER', next_opened_at) AS next_opened_at_quarter
FROM windowed
WHERE next_opened_at >= TIMESTAMP '2025-01-25 11:00:00'
ORDER BY id DESC, next_opened_at_quarter DESC
LIMIT 10
