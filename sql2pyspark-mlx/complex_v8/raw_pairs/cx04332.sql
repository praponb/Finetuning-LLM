WITH cleaned AS (
    -- Scope the tickets rows
    SELECT
        id,
        requester_id,
        agent_id,
        closed_at,
        channel,
        tags,
        CASE WHEN channel IN ('email', 'chat') THEN 'group_a' WHEN channel = 'web' THEN 'group_b' ELSE 'other' END AS channel_group,
        COALESCE(priority, 'UNKNOWN') AS priority_or_unknown,
        LENGTH(TRIM(channel)) AS channel_len
    FROM tickets
    WHERE (priority IS NOT NULL OR closed_at < TIMESTAMP '2025-02-20 15:00:00')
),
windowed AS (
    -- Running and moving aggregates
    SELECT
        *,
        channel_len - LAG(channel_len) OVER (PARTITION BY priority_or_unknown, agent_id ORDER BY requester_id ASC, id) AS channel_len_change,
        COUNT(*) OVER (PARTITION BY channel_len) AS rows_in_channel_len,
        ROUND(PERCENT_RANK() OVER (ORDER BY closed_at), 3) AS closed_at_percent_rank,
        ROUND(AVG(channel_len) OVER (PARTITION BY channel_len), 2) AS channel_len_avg_channel_len,
        LAST_VALUE(requester_id) OVER (PARTITION BY priority_or_unknown, agent_id ORDER BY requester_id ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_requester_id
    FROM cleaned
)
SELECT
    channel_len,
    COALESCE(priority_or_unknown, 'ALL') AS priority_or_unknown,
    agent_id,
    COUNT(*) AS row_count
FROM windowed
GROUP BY CUBE(channel_len, priority_or_unknown, agent_id)
ORDER BY channel_len NULLS LAST, priority_or_unknown NULLS LAST, agent_id NULLS LAST
