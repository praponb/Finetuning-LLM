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
    WHERE event_type <> 'replied'
),
with_prev AS (
    -- Previous event_at per actor
    SELECT *, LAG(event_at) OVER (PARTITION BY actor ORDER BY event_at, id) AS prev_event_at
    FROM recent_events
),
flagged AS (
    -- Flag rows that start a new session (gap > 90 minutes)
    SELECT *, CASE WHEN prev_event_at IS NULL OR event_at > prev_event_at + INTERVAL 90 MINUTES THEN 1 ELSE 0 END AS new_session_flag
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(new_session_flag) OVER (PARTITION BY actor ORDER BY event_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM flagged
),
actor_stats AS (
    -- Max new_session_flag per actor
    SELECT actor, MAX(new_session_flag) AS max_new_session_flag
    FROM sessionized
    GROUP BY actor
),
compared AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.max_new_session_flag,
        ROUND(s.new_session_flag - g.max_new_session_flag, 2) AS new_session_flag_minus_max
    FROM sessionized s
    JOIN actor_stats g ON g.actor = s.actor
    WHERE s.new_session_flag = g.max_new_session_flag
)
SELECT
    id,
    ticket_id,
    actor,
    actor_or_unknown,
    prev_event_at,
    new_session_flag,
    session_no,
    max_new_session_flag,
    ROUND(max_new_session_flag * 1.1, 2) AS max_new_session_flag_adj
FROM compared
ORDER BY actor DESC, id
LIMIT 20
