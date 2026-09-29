WITH recent AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor
    FROM ticket_events
),
with_prev AS (
    -- Previous event_at per actor
    SELECT *, LAG(event_at) OVER (PARTITION BY actor ORDER BY event_at, id) AS prev_event_at
    FROM recent
),
flagged AS (
    -- Flag rows that start a new session (gap > 90 minutes)
    SELECT *, CASE WHEN prev_event_at IS NULL OR event_at > prev_event_at + INTERVAL 90 MINUTES THEN 1 ELSE 0 END AS new_session_flag
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(new_session_flag) OVER (PARTITION BY actor ORDER BY event_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM flagged
),
per_session AS (
    -- One row per session
    SELECT
        actor,
        session_id,
        MIN(event_at) AS session_start,
        MAX(event_at) AS session_end,
        COUNT(*) AS n_events,
        ROUND((UNIX_TIMESTAMP(MAX(event_at)) - UNIX_TIMESTAMP(MIN(event_at))) / 60, 1) AS duration_min
    FROM sessionized
    GROUP BY actor, session_id
),
without_matches AS (
    -- Compare against the overall min
    SELECT *
    FROM per_session
    WHERE duration_min > (SELECT MIN(duration_min) FROM per_session)
),
ranked AS (
    -- Running and moving aggregates
    SELECT
        *,
        RANK() OVER w_actor_by_session_start_ties AS session_start_rank,
        ROW_NUMBER() OVER w_actor_by_session_start AS seq_no,
        LEAD(duration_min, 2, 0) OVER w_actor_by_session_start AS next2_duration_min,
        DENSE_RANK() OVER w_all_by_session_end_ties AS session_end_dense_rank
    FROM without_matches
    WINDOW w_actor_by_session_start_ties AS (PARTITION BY actor ORDER BY session_start DESC NULLS FIRST), w_actor_by_session_start AS (PARTITION BY actor ORDER BY session_start DESC NULLS FIRST, actor, session_id), w_all_by_session_end_ties AS (ORDER BY session_end)
)
-- Final projection
SELECT
    actor,
    session_id,
    session_start,
    n_events,
    duration_min,
    session_start_rank,
    seq_no,
    session_end_dense_rank,
    LOWER(REGEXP_REPLACE(TRIM(actor), ' ', '_')) AS actor_slug
FROM ranked
WHERE session_end_dense_rank <= 8
ORDER BY session_start DESC NULLS FIRST
