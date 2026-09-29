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
        csat,
        tags,
        CONCAT_WS('|', priority, channel) AS priority_channel_label
    FROM tickets
),
with_prev AS (
    -- Previous opened_at per priority
    SELECT *, LAG(opened_at) OVER (PARTITION BY priority ORDER BY opened_at, id) AS prev_opened_at
    FROM scoped_tickets
),
flagged AS (
    -- Flag rows that start a new session (gap > 45 minutes)
    SELECT *, CASE WHEN prev_opened_at IS NULL OR (UNIX_TIMESTAMP(opened_at) - UNIX_TIMESTAMP(prev_opened_at)) / 60 > 45 THEN 1 ELSE 0 END AS starts_session
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(starts_session) OVER (PARTITION BY priority ORDER BY opened_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM flagged
),
sessions AS (
    -- One row per session
    SELECT
        priority,
        session_id,
        MIN(opened_at) AS session_start,
        MAX(opened_at) AS session_end,
        COUNT(*) AS events,
        SUM(requester_id) AS session_requester_id,
        ROUND((UNIX_TIMESTAMP(MAX(opened_at)) - UNIX_TIMESTAMP(MIN(opened_at))) / 60, 1) AS duration_min
    FROM sessionized
    GROUP BY priority, session_id
),
with_trends AS (
    -- Window metrics over several partitions
    SELECT
        priority,
        session_id,
        session_start,
        session_end,
        duration_min,
        NTILE(4) OVER w_all_by_duration_min AS duration_min_quartile,
        SUM(session_id) OVER (PARTITION BY session_id, DATE_TRUNC('MONTH', session_end) ORDER BY session_requester_id NULLS LAST, priority, session_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_session_id,
        SUM(session_requester_id) OVER (PARTITION BY priority) AS priority_sum_session_requester_id,
        ROUND(duration_min / NULLIF(SUM(duration_min) OVER (PARTITION BY priority), 0), 4) AS duration_min_share_of_priority
    FROM sessions
    WINDOW w_all_by_duration_min AS (ORDER BY duration_min DESC NULLS FIRST, priority, session_id)
),
screened AS (
    -- Compare against the overall max
    SELECT *
    FROM with_trends
    WHERE running_session_id < (SELECT MAX(running_session_id) FROM with_trends)
)
-- Final projection
SELECT
    priority,
    session_id,
    session_start,
    session_end,
    duration_min,
    running_session_id,
    priority_sum_session_requester_id,
    duration_min_share_of_priority
FROM screened
ORDER BY duration_min_share_of_priority, session_end
