WITH prepared_events AS (
    -- Filter ticket_events
    SELECT *
    FROM ticket_events
),
with_prev AS (
    -- Previous event_at per actor
    SELECT *, LAG(event_at) OVER (PARTITION BY actor ORDER BY event_at, id) AS prev_event_at
    FROM prepared_events
),
flagged AS (
    -- Flag rows that start a new session (gap > 45 minutes)
    SELECT *, CASE WHEN prev_event_at IS NULL OR event_at > prev_event_at + INTERVAL 45 MINUTES THEN 1 ELSE 0 END AS is_new_session
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
summary AS (
    -- Group-level totals
    SELECT
        YEAR(session_start) AS session_start_year,
        actor,
        burst_id,
        SUM(CASE WHEN actor IN ('customer', 'agent') THEN 1 ELSE 0 END) AS n_actor_in_customer_agent,
        COUNT(DISTINCT actor) AS distinct_actors,
        MAX(session_start) AS last_session_start,
        SUM(CASE WHEN actor IS NOT NULL THEN duration_min ELSE 0 END) AS duration_min_actor_is_not_null
    FROM per_session
    GROUP BY 1, 2, 3
)
SELECT
    session_start_year,
    n_actor_in_customer_agent,
    distinct_actors,
    last_session_start,
    duration_min_actor_is_not_null,
    CASE WHEN actor IN ('system', 'customer') THEN 'group_a' WHEN actor = 'agent' THEN 'group_b' ELSE 'other' END AS actor_group
FROM summary
WHERE duration_min_actor_is_not_null > 480.0
ORDER BY last_session_start DESC, n_actor_in_customer_agent DESC
