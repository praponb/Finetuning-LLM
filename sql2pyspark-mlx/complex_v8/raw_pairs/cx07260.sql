WITH cleaned_events AS (
    -- Filter ticket_events
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor
    FROM ticket_events
    WHERE ticket_id >= 28
),
with_prev AS (
    -- Previous event_at per ticket_id
    SELECT *, LAG(event_at) OVER (PARTITION BY ticket_id ORDER BY event_at, id) AS prev_event_at
    FROM cleaned_events
),
flagged AS (
    -- Flag rows that start a new session (gap > 30 minutes)
    SELECT *, CASE WHEN prev_event_at IS NULL OR (UNIX_TIMESTAMP(event_at) - UNIX_TIMESTAMP(prev_event_at)) / 60 > 30 THEN 1 ELSE 0 END AS new_session_flag
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(new_session_flag) OVER (PARTITION BY ticket_id ORDER BY event_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM flagged
),
session_summary AS (
    -- One row per session
    SELECT
        ticket_id,
        session_id,
        MIN(event_at) AS session_start,
        MAX(event_at) AS session_end,
        COUNT(*) AS n_events,
        ROUND((UNIX_TIMESTAMP(MAX(event_at)) - UNIX_TIMESTAMP(MIN(event_at))) / 60, 1) AS duration_min
    FROM sessionized
    GROUP BY ticket_id, session_id
),
windowed AS (
    -- Window metrics over several partitions
    SELECT
        ticket_id,
        session_id,
        session_end,
        duration_min,
        MAX(duration_min) OVER (PARTITION BY YEAR(session_start), ticket_id ORDER BY n_events DESC, ticket_id, session_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_duration_min_so_far,
        SIZE(COLLECT_SET(ticket_id) OVER w_session_id_n_events) AS distinct_ticket_id_in_session_id_n_events,
        COUNT(*) OVER (PARTITION BY session_id, n_events) AS rows_in_session_id_n_events,
        NTILE(10) OVER (PARTITION BY YEAR(session_start), ticket_id ORDER BY n_events DESC, ticket_id, session_id) AS n_events_decile
    FROM session_summary
    WINDOW w_session_id_n_events AS (PARTITION BY session_id, n_events)
)
SELECT
    distinct_ticket_id_in_session_id_n_events,
    n_events_decile,
    ticket_id,
    COUNT(*) AS row_count,
    COUNT(DISTINCT ticket_id) AS distinct_ticket_ids
FROM windowed
GROUP BY GROUPING SETS ((distinct_ticket_id_in_session_id_n_events, n_events_decile, ticket_id), (distinct_ticket_id_in_session_id_n_events), ())
ORDER BY distinct_ticket_id_in_session_id_n_events NULLS LAST, n_events_decile NULLS LAST, ticket_id NULLS LAST
