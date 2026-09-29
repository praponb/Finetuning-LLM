WITH filtered AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        CASE WHEN event_type IN ('resolved', 'replied') THEN 'group_a' WHEN event_type = 'created' THEN 'group_b' ELSE 'other' END AS event_type_group
    FROM ticket_events
),
with_prev AS (
    -- Previous event_at per event_type_group
    SELECT *, LAG(event_at) OVER (PARTITION BY event_type_group ORDER BY event_at, id) AS prev_event_at
    FROM filtered
),
flagged AS (
    -- Flag rows that start a new session (gap > 90 minutes)
    SELECT *, CASE WHEN prev_event_at IS NULL OR event_at > prev_event_at + INTERVAL 90 MINUTES THEN 1 ELSE 0 END AS new_session_flag
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(new_session_flag) OVER (PARTITION BY event_type_group ORDER BY event_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM flagged
),
session_summary AS (
    -- One row per session
    SELECT
        event_type_group,
        session_no,
        MIN(event_at) AS session_start,
        MAX(event_at) AS session_end,
        COUNT(*) AS event_count,
        ROUND((UNIX_TIMESTAMP(MAX(event_at)) - UNIX_TIMESTAMP(MIN(event_at))) / 60, 1) AS duration_min
    FROM sessionized
    GROUP BY event_type_group, session_no
),
filtered_2 AS (
    -- Compare against the overall avg
    SELECT *
    FROM session_summary
    WHERE session_no > (SELECT AVG(session_no) FROM session_summary)
),
sequenced AS (
    -- Previous / next values per key
    SELECT
        event_type_group,
        session_no,
        session_start,
        session_end,
        duration_min,
        COUNT(*) OVER (PARTITION BY event_type_group) AS rows_in_event_type_group,
        ROUND(duration_min - AVG(duration_min) OVER w_event_type_group_2, 2) AS duration_min_vs_event_type_group_avg,
        ROUND(AVG(session_no) OVER (PARTITION BY event_type_group, event_count ORDER BY session_start ASC, event_type_group, session_no ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2) AS session_no_avg_3,
        SUM(event_count) OVER (PARTITION BY event_type_group) AS event_type_group_sum_event_count,
        RANK() OVER (PARTITION BY session_no, DATE_TRUNC('MONTH', session_start) ORDER BY event_count DESC) AS event_count_rank
    FROM filtered_2
    WINDOW w_event_type_group_2 AS (PARTITION BY event_type_group)
)
SELECT
    event_type_group,
    session_no,
    session_start,
    session_end,
    duration_min,
    duration_min_vs_event_type_group_avg,
    session_no_avg_3,
    event_type_group_sum_event_count,
    event_count_rank
FROM sequenced
ORDER BY session_no NULLS LAST, event_type_group
LIMIT 20
