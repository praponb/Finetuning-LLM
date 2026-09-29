WITH recent_tickets AS (
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
        tags
    FROM tickets
    WHERE opened_at < TIMESTAMP '2025-02-24 14:00:00'
),
with_prev AS (
    -- Previous opened_at per priority
    SELECT *, LAG(opened_at) OVER (PARTITION BY priority ORDER BY opened_at, id) AS prev_opened_at
    FROM recent_tickets
),
flagged AS (
    -- Flag rows that start a new session (gap > 45 minutes)
    SELECT *, CASE WHEN prev_opened_at IS NULL OR (UNIX_TIMESTAMP(opened_at) - UNIX_TIMESTAMP(prev_opened_at)) / 60 > 45 THEN 1 ELSE 0 END AS starts_session
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
        COUNT(*) AS event_count,
        ROUND((UNIX_TIMESTAMP(MAX(opened_at)) - UNIX_TIMESTAMP(MIN(opened_at))) / 60, 1) AS duration_min
    FROM sessionized
    GROUP BY priority, burst_id
),
with_activity AS (
    -- Compare against the overall max
    SELECT *
    FROM sessions
    WHERE burst_id < (SELECT MAX(burst_id) FROM sessions)
),
tagged AS (
    -- Add calculated columns
    SELECT
        priority,
        burst_id,
        session_start,
        session_end,
        duration_min,
        LEAST(burst_id, event_count) AS least_burst_id_event_count,
        LOWER(REGEXP_REPLACE(TRIM(priority), ' ', '_')) AS priority_slug
    FROM with_activity
    WHERE duration_min < 216.0
)
-- Final projection
SELECT
    priority,
    burst_id,
    session_end,
    duration_min,
    priority_slug,
    CONCAT_WS('-', priority_slug, priority) AS priority_slug_priority_label,
    COALESCE(burst_id, 0) AS burst_id_filled
FROM tagged
ORDER BY priority_slug_priority_label NULLS LAST
