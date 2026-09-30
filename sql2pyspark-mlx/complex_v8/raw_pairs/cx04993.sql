WITH recent_events AS (
    -- Scope the ticket_events rows
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        COALESCE(actor, 'UNKNOWN') AS actor_or_unknown
    FROM ticket_events
    WHERE TO_DATE(event_at) < DATE '2025-02-06'
),
with_prev AS (
    -- Previous event_at per ticket_id
    SELECT *, LAG(event_at) OVER (PARTITION BY ticket_id ORDER BY event_at, id) AS prev_event_at
    FROM recent_events
),
flagged AS (
    -- Flag rows that start a new session (gap > 30 minutes)
    SELECT *, CASE WHEN prev_event_at IS NULL OR (UNIX_TIMESTAMP(event_at) - UNIX_TIMESTAMP(prev_event_at)) / 60 > 30 THEN 1 ELSE 0 END AS is_new_session
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_session) OVER (PARTITION BY ticket_id ORDER BY event_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM flagged
),
sessions AS (
    -- One row per session
    SELECT
        ticket_id,
        session_no,
        MIN(event_at) AS session_start,
        MAX(event_at) AS session_end,
        COUNT(*) AS event_count,
        ROUND((UNIX_TIMESTAMP(MAX(event_at)) - UNIX_TIMESTAMP(MIN(event_at))) / 60, 1) AS duration_min
    FROM sessionized
    GROUP BY ticket_id, session_no
),
ranked AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        RANK() OVER w_all_by_session_end AS session_end_rank,
        ROW_NUMBER() OVER (ORDER BY session_end NULLS LAST, ticket_id, session_no) AS session_end_seq
    FROM sessions
    WINDOW w_all_by_session_end AS (ORDER BY session_end NULLS LAST, ticket_id, session_no)
),
metrics AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROUND(AVG(session_end_rank) OVER w_ticket_id_2, 2) AS ticket_id_avg_session_end_rank,
        ROW_NUMBER() OVER (PARTITION BY session_no, session_end_seq ORDER BY session_start, ticket_id, session_no) AS seq_no
    FROM ranked
    WINDOW w_ticket_id_2 AS (PARTITION BY ticket_id)
)
SELECT
    ticket_id,
    session_no,
    session_start,
    session_end,
    event_count,
    session_end_rank,
    session_end_seq,
    ticket_id_avg_session_end_rank,
    seq_no,
    ROUND((UNIX_TIMESTAMP(session_start) - UNIX_TIMESTAMP(session_end)) / 60, 1) AS minutes_session_end_to_session_start,
    DATE_FORMAT(session_end, 'MMM yyyy') AS session_end_label
FROM metrics
WHERE session_end_seq > 3
