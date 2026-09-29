WITH base_events AS (
    -- Filter ticket_events
    SELECT *
    FROM ticket_events
    WHERE ticket_id >= 3
),
with_prev AS (
    -- Previous event_at per actor
    SELECT *, LAG(event_at) OVER (PARTITION BY actor ORDER BY event_at, id) AS prev_event_at
    FROM base_events
),
flagged AS (
    -- Flag rows that start a new session (gap > 90 minutes)
    SELECT *, CASE WHEN prev_event_at IS NULL OR (UNIX_TIMESTAMP(event_at) - UNIX_TIMESTAMP(prev_event_at)) / 60 > 90 THEN 1 ELSE 0 END AS is_new_session
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_session) OVER (PARTITION BY actor ORDER BY event_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM flagged
),
sessions AS (
    -- One row per session
    SELECT
        actor,
        session_id,
        MIN(event_at) AS session_start,
        MAX(event_at) AS session_end,
        COUNT(*) AS events,
        ROUND((UNIX_TIMESTAMP(MAX(event_at)) - UNIX_TIMESTAMP(MIN(event_at))) / 60, 1) AS duration_min
    FROM sessionized
    GROUP BY actor, session_id
),
with_activity AS (
    -- Compare against the overall avg
    SELECT *
    FROM sessions
    WHERE session_id > (SELECT AVG(session_id) FROM sessions)
)
SELECT
    actor,
    session_id,
    events,
    COUNT(DISTINCT actor) AS distinct_actors,
    SUM(CASE WHEN actor <> 'agent' THEN session_id ELSE 0 END) AS session_id_actor_agent
FROM with_activity
GROUP BY GROUPING SETS ((actor, session_id, events), (actor), (session_id), ())
ORDER BY actor NULLS LAST, session_id NULLS LAST, events NULLS LAST
