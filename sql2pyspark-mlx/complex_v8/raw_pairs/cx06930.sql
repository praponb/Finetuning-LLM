WITH recent_events AS (
    -- Scope the ticket_events rows
    SELECT *
    FROM ticket_events
),
with_prev AS (
    -- Previous event_at per actor
    SELECT *, LAG(event_at) OVER (PARTITION BY actor ORDER BY event_at, id) AS prev_event_at
    FROM recent_events
),
flagged AS (
    -- Flag rows that start a new session (gap > 90 minutes)
    SELECT *, CASE WHEN prev_event_at IS NULL OR event_at > prev_event_at + INTERVAL 90 MINUTES THEN 1 ELSE 0 END AS is_new_session
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_session) OVER (PARTITION BY actor ORDER BY event_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM flagged
),
per_session AS (
    -- One row per session
    SELECT
        actor,
        burst_id,
        MIN(event_at) AS session_start,
        MAX(event_at) AS session_end,
        COUNT(*) AS events,
        ROUND((UNIX_TIMESTAMP(MAX(event_at)) - UNIX_TIMESTAMP(MIN(event_at))) / 60, 1) AS duration_min
    FROM sessionized
    GROUP BY actor, burst_id
),
ranked AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(AVG(duration_min) OVER (ORDER BY session_end ASC NULLS LAST, actor, burst_id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING), 2) AS duration_min_avg_3_prior,
        ROUND(AVG(events) OVER (PARTITION BY actor), 2) AS actor_avg_events,
        LAST_VALUE(session_end) IGNORE NULLS OVER (ORDER BY duration_min DESC NULLS FIRST, actor, burst_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_session_end,
        NTH_VALUE(session_end, 2) OVER (ORDER BY session_end ASC NULLS LAST, actor, burst_id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_session_end,
        ROUND((UNIX_TIMESTAMP(session_start) - UNIX_TIMESTAMP(LAG(session_start) OVER w_all_by_session_end)) / 60, 1) AS minutes_since_prev_session_start
    FROM per_session
    WINDOW w_all_by_session_end AS (ORDER BY session_end ASC NULLS LAST, actor, burst_id)
)
SELECT
    COALESCE(actor, 'ALL') AS actor,
    burst_id,
    events,
    GROUPING_ID() AS gid,
    SUM(actor_avg_events) AS total_actor_avg_events,
    COUNT(*) AS row_count,
    SUM(events) FILTER (WHERE actor IS NOT NULL) AS events_actor_is_not_null
FROM ranked
GROUP BY ROLLUP(actor, burst_id, events)
ORDER BY actor NULLS LAST, burst_id NULLS LAST, events NULLS LAST
