WITH filtered AS (
    -- Scope the ticket_events rows
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        COALESCE(event_type, 'UNKNOWN') AS event_type_or_unknown
    FROM ticket_events
),
with_prev AS (
    -- Previous event_at per event_type
    SELECT *, LAG(event_at) OVER (PARTITION BY event_type ORDER BY event_at, id) AS prev_event_at
    FROM filtered
),
flagged AS (
    -- Flag rows that start a new session (gap > 30 minutes)
    SELECT *, CASE WHEN prev_event_at IS NULL OR event_at > prev_event_at + INTERVAL 30 MINUTES THEN 1 ELSE 0 END AS is_new_session
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_session) OVER (PARTITION BY event_type ORDER BY event_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM flagged
),
sessions AS (
    -- One row per session
    SELECT
        event_type,
        session_id,
        MIN(event_at) AS session_start,
        MAX(event_at) AS session_end,
        COUNT(*) AS event_count,
        ROUND((UNIX_TIMESTAMP(MAX(event_at)) - UNIX_TIMESTAMP(MIN(event_at))) / 60, 1) AS duration_min
    FROM sessionized
    GROUP BY event_type, session_id
),
windowed AS (
    -- Previous / next values per key
    SELECT
        event_type,
        session_id,
        session_start,
        event_count,
        duration_min,
        ROUND(session_id - AVG(session_id) OVER w_session_start_to_date_sessio, 2) AS session_id_vs_session_start_to_date_session_id_a,
        DENSE_RANK() OVER (PARTITION BY event_type ORDER BY event_count ASC, event_type, session_id) AS event_count_dense_rank,
        ROUND(CUME_DIST() OVER (PARTITION BY event_type ORDER BY event_count ASC), 3) AS event_count_cume_dist,
        ROUND(PERCENT_RANK() OVER w_event_type_by_event_count_ti, 3) AS event_count_percent_rank,
        NTH_VALUE(session_id, 2) OVER (PARTITION BY event_type ORDER BY event_count ASC, event_type, session_id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_session_id
    FROM sessions
    WINDOW w_session_start_to_date_sessio AS (PARTITION BY TO_DATE(session_start), session_id), w_event_type_by_event_count_ti AS (PARTITION BY event_type ORDER BY event_count ASC)
),
per_group AS (
    -- Summarise per group
    SELECT
        DATE_TRUNC('MONTH', session_start) AS session_start_month,
        session_id,
        event_count_dense_rank,
        ARRAY_SORT(COLLECT_SET(event_type)) AS event_type_list,
        COUNT(*) AS row_count,
        ROUND(AVG(duration_min), 2) AS avg_duration_min,
        MAX(event_count) AS max_event_count
    FROM windowed
    GROUP BY DATE_TRUNC('MONTH', session_start), session_id, event_count_dense_rank
)
-- Final projection
SELECT
    session_start_month,
    session_id,
    event_count_dense_rank,
    event_type_list,
    max_event_count,
    ARRAY_JOIN(ARRAY_SORT(event_type_list), ',') AS event_type_list_text,
    DATEDIFF(DATE '2025-12-31', session_start_month) AS days_since_session_start_month
FROM per_group
ORDER BY event_count_dense_rank, event_type_list_text
