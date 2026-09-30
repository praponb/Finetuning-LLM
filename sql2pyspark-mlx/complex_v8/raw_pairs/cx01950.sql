WITH valid_events AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        CONCAT_WS('|', event_type, actor) AS event_type_actor_label
    FROM ticket_events
    WHERE actor = 'customer'
),
with_prev AS (
    -- Previous event_at per actor
    SELECT *, LAG(event_at) OVER (PARTITION BY actor ORDER BY event_at, id) AS prev_event_at
    FROM valid_events
),
flagged AS (
    -- Flag rows that start a new session (gap > 30 minutes)
    SELECT *, CASE WHEN prev_event_at IS NULL OR event_at > prev_event_at + INTERVAL 30 MINUTES THEN 1 ELSE 0 END AS is_new_session
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_session) OVER (PARTITION BY actor ORDER BY event_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM flagged
),
session_summary AS (
    -- One row per session
    SELECT
        actor,
        session_id,
        MIN(event_at) AS session_start,
        MAX(event_at) AS session_end,
        COUNT(*) AS event_count,
        ROUND((UNIX_TIMESTAMP(MAX(event_at)) - UNIX_TIMESTAMP(MIN(event_at))) / 60, 1) AS duration_min
    FROM sessionized
    GROUP BY actor, session_id
),
labeled AS (
    -- Add calculated columns
    SELECT
        *,
        duration_min + session_id AS duration_min_plus_session_id,
        CASE WHEN duration_min >= 192.0 THEN 'large' WHEN duration_min >= 108.0 THEN 'medium' ELSE 'small' END AS duration_min_tier
    FROM session_summary
),
enriched AS (
    -- Add calculated columns
    SELECT
        *,
        CASE WHEN duration_min_tier IN ('medium', 'small') THEN 'group_a' WHEN duration_min_tier = 'large' THEN 'group_b' ELSE 'other' END AS duration_min_tier_group,
        session_start + INTERVAL 1 DAYS AS session_start_plus_1d
    FROM labeled
    WHERE session_start >= TIMESTAMP '2025-01-09 09:00:00'
)
-- Final projection
SELECT
    actor,
    session_start,
    session_end,
    duration_min,
    duration_min_plus_session_id,
    duration_min_tier,
    duration_min_tier_group,
    session_start_plus_1d,
    ABS(duration_min - 144.0) AS duration_min_distance
FROM enriched
ORDER BY actor DESC
