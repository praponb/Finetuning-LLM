WITH filtered_tickets AS (
    -- Filter tickets
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
    WHERE ARRAY_CONTAINS(tags, 'refund')
),
with_prev AS (
    -- Previous opened_at per priority
    SELECT *, LAG(opened_at) OVER (PARTITION BY priority ORDER BY opened_at, id) AS prev_opened_at
    FROM filtered_tickets
),
flagged AS (
    -- Flag rows that start a new session (gap > 90 minutes)
    SELECT *, CASE WHEN prev_opened_at IS NULL OR (UNIX_TIMESTAMP(opened_at) - UNIX_TIMESTAMP(prev_opened_at)) / 60 > 90 THEN 1 ELSE 0 END AS starts_session
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(starts_session) OVER (PARTITION BY priority ORDER BY opened_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM flagged
),
per_session AS (
    -- One row per session
    SELECT
        priority,
        session_id,
        MIN(opened_at) AS session_start,
        MAX(opened_at) AS session_end,
        COUNT(*) AS event_count,
        ROUND((UNIX_TIMESTAMP(MAX(opened_at)) - UNIX_TIMESTAMP(MIN(opened_at))) / 60, 1) AS duration_min
    FROM sessionized
    GROUP BY priority, session_id
),
annotated AS (
    -- Derive helper columns
    SELECT
        *,
        LENGTH(TRIM(priority)) AS priority_len,
        CASE WHEN event_count >= 5 THEN 'large' WHEN event_count >= 3 THEN 'medium' ELSE 'small' END AS event_count_tier,
        CASE WHEN priority IN ('P4', 'P3') THEN 'group_a' WHEN priority = 'P1' THEN 'group_b' ELSE 'other' END AS priority_group
    FROM per_session
    WHERE session_id <= 5
)
SELECT
    priority,
    session_id,
    session_end,
    event_count,
    duration_min,
    priority_group,
    QUARTER(session_start) AS session_start_quarter
FROM annotated
ORDER BY session_id, priority_group DESC
