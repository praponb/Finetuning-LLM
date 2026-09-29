WITH scoped AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor
    FROM ticket_events
    WHERE event_at >= TIMESTAMP '2025-01-13 14:00:00'
),
with_prev AS (
    -- Previous event_at per actor
    SELECT *, LAG(event_at) OVER (PARTITION BY actor ORDER BY event_at, id) AS prev_event_at
    FROM scoped
),
flagged AS (
    -- Flag rows that start a new session (gap > 2 hours)
    SELECT *, CASE WHEN prev_event_at IS NULL OR event_at > prev_event_at + INTERVAL 2 HOURS THEN 1 ELSE 0 END AS new_session_flag
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(new_session_flag) OVER (PARTITION BY actor ORDER BY event_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM flagged
),
metrics AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        MAX(session_id) OVER (PARTITION BY actor ORDER BY new_session_flag ASC, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS session_id_max_2_prior,
        ROW_NUMBER() OVER (PARTITION BY new_session_flag, session_id ORDER BY event_at, id) AS event_at_seq
    FROM sessionized
),
latest_per_key AS (
    -- Keep the latest row per event_type
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        prev_event_at,
        new_session_flag,
        session_id,
        session_id_max_2_prior,
        event_at_seq
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY event_type ORDER BY session_id DESC, id DESC) AS row_num
        FROM metrics
    ) ranked
    WHERE row_num = 1
)
SELECT
    id,
    ticket_id,
    event_at,
    actor,
    prev_event_at,
    new_session_flag,
    session_id_max_2_prior
FROM latest_per_key
