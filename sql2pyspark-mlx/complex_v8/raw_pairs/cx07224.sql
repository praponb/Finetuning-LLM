WITH recent_events AS (
    -- Scope the ticket_events rows
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor
    FROM ticket_events
    WHERE event_at < TIMESTAMP '2025-03-08 09:00:00'
),
with_prev AS (
    -- Previous event_at per ticket_id
    SELECT *, LAG(event_at) OVER (PARTITION BY ticket_id ORDER BY event_at, id) AS prev_event_at
    FROM recent_events
),
flagged AS (
    -- Flag rows that start a new session (gap > 60 minutes)
    SELECT *, CASE WHEN prev_event_at IS NULL OR event_at > prev_event_at + INTERVAL 60 MINUTES THEN 1 ELSE 0 END AS new_session_flag
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(new_session_flag) OVER (PARTITION BY ticket_id ORDER BY event_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM flagged
),
event_type_stats AS (
    -- Avg new_session_flag per event_type
    SELECT event_type, AVG(new_session_flag) AS avg_new_session_flag
    FROM sessionized
    GROUP BY event_type
),
vs_group AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_new_session_flag,
        ROUND(s.new_session_flag - g.avg_new_session_flag, 2) AS new_session_flag_minus_avg
    FROM sessionized s
    JOIN event_type_stats g ON g.event_type = s.event_type
    WHERE s.new_session_flag > g.avg_new_session_flag
),
labeled AS (
    -- Label and score rows
    SELECT *, DATE_FORMAT(event_at, 'MMM yyyy') AS event_at_label
    FROM vs_group
)
SELECT
    id,
    ticket_id,
    actor,
    prev_event_at,
    new_session_flag,
    session_no,
    avg_new_session_flag,
    event_at_label
FROM labeled
WHERE session_no < 4
ORDER BY session_no DESC, id DESC
