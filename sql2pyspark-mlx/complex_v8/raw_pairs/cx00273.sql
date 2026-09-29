WITH valid AS (
    -- Scope the ticket_events rows
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor
    FROM ticket_events
    WHERE ticket_id >= 20
),
with_prev AS (
    -- Previous event_at per event_type
    SELECT *, LAG(event_at) OVER (PARTITION BY event_type ORDER BY event_at, id) AS prev_event_at
    FROM valid
),
flagged AS (
    -- Flag rows that start a new session (gap > 45 minutes)
    SELECT *, CASE WHEN prev_event_at IS NULL OR event_at > prev_event_at + INTERVAL 45 MINUTES THEN 1 ELSE 0 END AS starts_session
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(starts_session) OVER (PARTITION BY event_type ORDER BY event_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM flagged
),
line_level_events AS (
    -- One row per event
    SELECT
        s.id,
        s.ticket_id,
        s.event_at,
        s.prev_event_at,
        s.starts_session,
        t.id AS event_id,
        t.actor AS event_actor,
        t.event_at AS event_event_at
    FROM sessionized s
    JOIN ticket_events t ON t.ticket_id = s.ticket_id
),
event_actor_stats AS (
    -- Median starts_session per event_actor
    SELECT event_actor, PERCENTILE_APPROX(starts_session, 0.5) AS median_starts_session
    FROM line_level_events
    GROUP BY event_actor
),
vs_group AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.median_starts_session,
        ROUND(s.starts_session - g.median_starts_session, 2) AS starts_session_minus_median
    FROM line_level_events s
    JOIN event_actor_stats g ON g.event_actor = s.event_actor
    WHERE s.starts_session >= g.median_starts_session
)
-- Final projection
SELECT
    id,
    ticket_id,
    event_at,
    prev_event_at,
    starts_session,
    event_id,
    event_actor,
    event_event_at,
    starts_session_minus_median
FROM vs_group
