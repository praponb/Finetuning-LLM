WITH valid AS (
    -- Keep valid rows and derive helper columns
    SELECT *
    FROM ticket_events
),
with_prev AS (
    -- Previous event_at per actor
    SELECT *, LAG(event_at) OVER (PARTITION BY actor ORDER BY event_at, id) AS prev_event_at
    FROM valid
),
flagged AS (
    -- Flag rows that start a new session (gap > 2 hours)
    SELECT *, CASE WHEN prev_event_at IS NULL OR (UNIX_TIMESTAMP(event_at) - UNIX_TIMESTAMP(prev_event_at)) / 60 > 120 THEN 1 ELSE 0 END AS starts_session
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(starts_session) OVER (PARTITION BY actor ORDER BY event_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM flagged
),
session_summary AS (
    -- One row per session
    SELECT
        actor,
        session_no,
        MIN(event_at) AS session_start,
        MAX(event_at) AS session_end,
        COUNT(*) AS n_events,
        ROUND((UNIX_TIMESTAMP(MAX(event_at)) - UNIX_TIMESTAMP(MIN(event_at))) / 60, 1) AS duration_min
    FROM sessionized
    GROUP BY actor, session_no
),
scored AS (
    -- Derive helper columns
    SELECT
        *,
        session_start + INTERVAL 1 DAYS AS session_start_plus_1d,
        CONCAT(actor, '#', CAST(n_events AS STRING)) AS actor_ref
    FROM session_summary
)
-- Final projection
SELECT
    actor,
    session_no,
    session_end,
    n_events,
    duration_min,
    session_start_plus_1d,
    actor_ref,
    ROUND(duration_min / NULLIF(session_no, 0), 3) AS duration_min_per_session_no
FROM scored
ORDER BY duration_min DESC, session_no NULLS LAST
