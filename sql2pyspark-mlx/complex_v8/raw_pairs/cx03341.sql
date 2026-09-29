WITH cleaned_events AS (
    -- Scope the ticket_events rows
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        CASE WHEN actor IN ('customer', 'system') THEN 'group_a' WHEN actor = 'agent' THEN 'group_b' ELSE 'other' END AS actor_group
    FROM ticket_events
    WHERE ticket_id IS NOT NULL
),
with_prev AS (
    -- Previous event_at per actor
    SELECT *, LAG(event_at) OVER (PARTITION BY actor ORDER BY event_at, id) AS prev_event_at
    FROM cleaned_events
),
flagged AS (
    -- Flag rows that start a new session (gap > 45 minutes)
    SELECT *, CASE WHEN prev_event_at IS NULL OR event_at > prev_event_at + INTERVAL 45 MINUTES THEN 1 ELSE 0 END AS is_new_session
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_session) OVER (PARTITION BY actor ORDER BY event_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM flagged
),
per_session AS (
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
ranked AS (
    -- Previous / next values per key
    SELECT
        *,
        ROUND(duration_min - AVG(duration_min) OVER w_session_start_to_date_sessio, 2) AS duration_min_vs_session_start_to_date_session_no,
        ROUND(duration_min / NULLIF(SUM(duration_min) OVER w_session_start_to_date_sessio, 0), 4) AS duration_min_share_of_session_start_to_date_sess,
        LAG(n_events) OVER (PARTITION BY n_events ORDER BY n_events DESC, actor, session_no) AS prev_n_events,
        NTILE(5) OVER (PARTITION BY n_events ORDER BY n_events DESC, actor, session_no) AS n_events_quintile,
        FIRST_VALUE(session_no) OVER (PARTITION BY n_events ORDER BY n_events DESC, actor, session_no ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_session_no
    FROM per_session
    WINDOW w_session_start_to_date_sessio AS (PARTITION BY TO_DATE(session_start), session_no)
),
summary AS (
    -- Group-level totals
    SELECT
        first_session_no,
        n_events_quintile,
        actor,
        SUM(CASE WHEN actor <> 'system' THEN prev_n_events ELSE 0 END) AS prev_n_events_actor_system,
        COUNT(DISTINCT actor) AS distinct_actors,
        ROUND(AVG(duration_min_vs_session_start_to_date_session_no), 2) AS avg_duration_min_vs_session_start_to_date_sessio
    FROM ranked
    GROUP BY first_session_no, n_events_quintile, actor
)
SELECT
    first_session_no,
    n_events_quintile,
    actor,
    distinct_actors,
    ABS(n_events_quintile - 5) AS n_events_quintile_distance,
    LENGTH(TRIM(actor)) AS actor_len
FROM summary
ORDER BY first_session_no
