WITH cleaned_events AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        UPPER(TRIM(event_type)) AS event_type_clean
    FROM ticket_events
    WHERE event_at < TIMESTAMP '2025-03-02 09:00:00'
),
with_prev AS (
    -- Previous event_at per ticket_id
    SELECT *, LAG(event_at) OVER (PARTITION BY ticket_id ORDER BY event_at, id) AS prev_event_at
    FROM cleaned_events
),
flagged AS (
    -- Flag rows that start a new session (gap > 30 minutes)
    SELECT *, CASE WHEN prev_event_at IS NULL OR (UNIX_TIMESTAMP(event_at) - UNIX_TIMESTAMP(prev_event_at)) / 60 > 30 THEN 1 ELSE 0 END AS starts_session
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(starts_session) OVER (PARTITION BY ticket_id ORDER BY event_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
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
sequenced AS (
    -- Window metrics over several partitions
    SELECT
        *,
        SUM(session_no) OVER (PARTITION BY session_no, DATE_TRUNC('MONTH', session_start)) AS session_no_session_start_month_sum_session_no,
        NTILE(10) OVER (ORDER BY session_start DESC, ticket_id, session_no) AS session_start_decile,
        ROUND(CUME_DIST() OVER (ORDER BY events, ticket_id, session_no), 3) AS events_cume_dist,
        ROUND(PERCENT_RANK() OVER (ORDER BY events), 3) AS events_percent_rank
    FROM sessions
)
SELECT
    session_no,
    ticket_id,
    session_start_decile,
    GROUPING(session_no) AS is_total_session_no,
    MAX(session_start) AS last_session_start,
    ROUND(STDDEV(events), 2) AS stddev_events
FROM sequenced
GROUP BY GROUPING SETS ((session_no, ticket_id, session_start_decile), (session_no), ())
ORDER BY session_no NULLS LAST, ticket_id NULLS LAST, session_start_decile NULLS LAST
