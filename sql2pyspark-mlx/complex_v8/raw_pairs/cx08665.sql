WITH base AS (
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
    -- Previous event_at per event_type
    SELECT *, LAG(event_at) OVER (PARTITION BY event_type ORDER BY event_at, id) AS prev_event_at
    FROM base
),
flagged AS (
    -- Flag rows that start a new session (gap > 2 hours)
    SELECT *, CASE WHEN prev_event_at IS NULL OR (UNIX_TIMESTAMP(event_at) - UNIX_TIMESTAMP(prev_event_at)) / 60 > 120 THEN 1 ELSE 0 END AS is_new_session
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_session) OVER (PARTITION BY event_type ORDER BY event_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM flagged
),
ranked AS (
    -- Running and moving aggregates
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        session_id,
        MIN(session_id) OVER (ORDER BY event_at ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_session_id_so_far,
        RANK() OVER w_all_by_event_at_ties AS event_at_rank,
        NTILE(3) OVER (ORDER BY event_at ASC, id) AS event_at_tercile,
        RANK() OVER (ORDER BY event_at ASC, id) AS event_at_rank_2,
        COUNT(*) OVER (PARTITION BY event_type, TO_DATE(prev_event_at)) AS rows_in_event_type_prev_event_at_to_date,
        MAX(is_new_session) OVER w_event_type_prev_event_at_to AS event_type_prev_event_at_to_date_max_is_new_sess
    FROM sessionized
    WINDOW w_all_by_event_at_ties AS (ORDER BY event_at ASC), w_event_type_prev_event_at_to AS (PARTITION BY event_type, TO_DATE(prev_event_at))
)
-- Final projection
SELECT
    id,
    ticket_id,
    event_at,
    session_id,
    event_at_rank,
    event_at_tercile,
    event_at_rank_2,
    rows_in_event_type_prev_event_at_to_date,
    event_type_prev_event_at_to_date_max_is_new_sess,
    ABS(event_at_tercile - 4) AS event_at_tercile_distance,
    ABS(session_id - 2) AS session_id_distance
FROM ranked
ORDER BY session_id DESC
