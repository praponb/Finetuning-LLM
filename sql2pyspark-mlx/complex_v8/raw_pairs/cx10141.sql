WITH base_tickets AS (
    -- Scope the tickets rows
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
        requester_id - csat AS requester_id_minus_csat
    FROM tickets
),
with_prev AS (
    -- Previous opened_at per priority
    SELECT *, LAG(opened_at) OVER (PARTITION BY priority ORDER BY opened_at, id) AS prev_opened_at
    FROM base_tickets
),
flagged AS (
    -- Flag rows that start a new session (gap > 90 minutes)
    SELECT *, CASE WHEN prev_opened_at IS NULL OR opened_at > prev_opened_at + INTERVAL 90 MINUTES THEN 1 ELSE 0 END AS starts_session
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(starts_session) OVER (PARTITION BY priority ORDER BY opened_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM flagged
),
sessions AS (
    -- One row per session
    SELECT
        priority,
        burst_id,
        MIN(opened_at) AS session_start,
        MAX(opened_at) AS session_end,
        COUNT(*) AS events,
        SUM(requester_id_minus_csat) AS session_requester_id_minus_csat,
        ROUND((UNIX_TIMESTAMP(MAX(opened_at)) - UNIX_TIMESTAMP(MIN(opened_at))) / 60, 1) AS duration_min
    FROM sessionized
    GROUP BY priority, burst_id
),
annotated AS (
    -- Label and score rows
    SELECT
        priority,
        burst_id,
        session_start,
        session_end,
        events,
        session_requester_id_minus_csat,
        TO_DATE(session_start) AS session_start_date
    FROM sessions
    WHERE session_end >= TIMESTAMP '2025-01-21 09:00:00'
),
sequenced AS (
    -- Running and moving aggregates
    SELECT
        *,
        SIZE(COLLECT_SET(priority) OVER (PARTITION BY events, DATE_TRUNC('MONTH', session_start))) AS distinct_priority_in_events_session_start_month,
        COUNT(*) OVER (ORDER BY session_end DESC, priority, burst_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count,
        MAX(burst_id) OVER (ORDER BY session_end DESC, priority, burst_id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS burst_id_max_2_prior,
        NTH_VALUE(session_start_date, 2) OVER (ORDER BY session_end DESC, priority, burst_id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_session_start_date
    FROM annotated
)
-- Subtotals with GROUPING SETS
SELECT
    priority,
    burst_id,
    COUNT(*) FILTER (WHERE priority = 'P3') AS n_priority_p3
FROM sequenced
GROUP BY GROUPING SETS ((priority, burst_id), (priority), ())
ORDER BY priority NULLS LAST, burst_id NULLS LAST
