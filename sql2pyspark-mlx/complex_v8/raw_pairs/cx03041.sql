WITH cleaned_events AS (
    -- Filter ticket_events
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        LOWER(REGEXP_REPLACE(TRIM(actor), ' ', '_')) AS actor_slug
    FROM ticket_events
    WHERE ticket_id >= 39
),
with_prev AS (
    -- Previous event_at per ticket_id
    SELECT *, LAG(event_at) OVER (PARTITION BY ticket_id ORDER BY event_at, id) AS prev_event_at
    FROM cleaned_events
),
flagged AS (
    -- Flag rows that start a new session (gap > 90 minutes)
    SELECT *, CASE WHEN prev_event_at IS NULL OR event_at > prev_event_at + INTERVAL 90 MINUTES THEN 1 ELSE 0 END AS new_session_flag
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(new_session_flag) OVER (PARTITION BY ticket_id ORDER BY event_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM flagged
),
sessions AS (
    -- One row per session
    SELECT
        ticket_id,
        session_no,
        MIN(event_at) AS session_start,
        MAX(event_at) AS session_end,
        COUNT(*) AS events,
        ROUND((UNIX_TIMESTAMP(MAX(event_at)) - UNIX_TIMESTAMP(MIN(event_at))) / 60, 1) AS duration_min
    FROM sessionized
    GROUP BY ticket_id, session_no
),
windowed AS (
    -- Window metrics over several partitions
    SELECT
        ticket_id,
        session_no,
        session_start,
        duration_min,
        RANK() OVER (PARTITION BY ticket_id, events ORDER BY events DESC, ticket_id, session_no) AS events_rank,
        MIN(duration_min) OVER (PARTITION BY ticket_id, events ORDER BY events DESC, ticket_id, session_no ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_duration_min_so_far,
        COUNT(*) OVER (PARTITION BY events) AS rows_in_events,
        SUM(events) OVER (PARTITION BY ticket_id, events ORDER BY events DESC, ticket_id, session_no ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS events_sum_2
    FROM sessions
)
SELECT
    ticket_id,
    session_no,
    session_start,
    duration_min,
    events_rank,
    min_duration_min_so_far,
    rows_in_events,
    events_sum_2
FROM windowed
ORDER BY ticket_id NULLS LAST, rows_in_events NULLS LAST
