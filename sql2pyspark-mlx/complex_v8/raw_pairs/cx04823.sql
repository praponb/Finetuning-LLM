WITH filtered AS (
    -- Scope the tickets rows
    SELECT
        id,
        agent_id,
        opened_at,
        closed_at,
        channel,
        csat,
        tags
    FROM tickets
    WHERE ARRAY_CONTAINS(tags, 'bug')
),
with_prev AS (
    -- Previous opened_at per channel
    SELECT *, LAG(opened_at) OVER (PARTITION BY channel ORDER BY opened_at, id) AS prev_opened_at
    FROM filtered
),
flagged AS (
    -- Flag rows that start a new session (gap > 60 minutes)
    SELECT *, CASE WHEN prev_opened_at IS NULL OR opened_at > prev_opened_at + INTERVAL 60 MINUTES THEN 1 ELSE 0 END AS new_session_flag
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(new_session_flag) OVER (PARTITION BY channel ORDER BY opened_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM flagged
),
per_session AS (
    -- One row per session
    SELECT
        channel,
        session_id,
        MIN(opened_at) AS session_start,
        MAX(opened_at) AS session_end,
        COUNT(*) AS events,
        ROUND((UNIX_TIMESTAMP(MAX(opened_at)) - UNIX_TIMESTAMP(MIN(opened_at))) / 60, 1) AS duration_min
    FROM sessionized
    GROUP BY channel, session_id
),
totals AS (
    -- Aggregate with conditional metrics
    SELECT
        DATE_TRUNC('MONTH', session_end) AS session_end_month,
        events,
        ROUND(SUM(duration_min), 2) AS total_duration_min,
        COUNT(*) FILTER (WHERE channel IS NOT NULL) AS n_channel_is_not_null
    FROM per_session
    GROUP BY DATE_TRUNC('MONTH', session_end), events
),
with_trends AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        MAX(events) OVER (ORDER BY total_duration_min ASC, session_end_month, events ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS events_max_3,
        NTILE(3) OVER w_all_by_total_duration_min AS total_duration_min_tercile,
        SUM(total_duration_min) OVER (PARTITION BY n_channel_is_not_null, events) AS n_channel_is_not_null_events_sum_total_duration_,
        SUM(total_duration_min) OVER (ORDER BY total_duration_min ASC, session_end_month, events ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS total_duration_min_sum_6_prior
    FROM totals
    WINDOW w_all_by_total_duration_min AS (ORDER BY total_duration_min ASC, session_end_month, events)
)
-- Final projection
SELECT
    session_end_month,
    events,
    total_duration_min,
    n_channel_is_not_null,
    events_max_3,
    total_duration_min_tercile,
    n_channel_is_not_null_events_sum_total_duration_,
    total_duration_min_sum_6_prior,
    CASE WHEN total_duration_min >= 864.0 THEN 'large' WHEN total_duration_min >= 360.0 THEN 'medium' ELSE 'small' END AS total_duration_min_tier
FROM with_trends
ORDER BY session_end_month DESC NULLS FIRST, events
LIMIT 10
