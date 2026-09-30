WITH recent_enrollments AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        enrolled_at
    FROM enrollments
),
with_prev AS (
    -- Previous enrolled_at per course_id
    SELECT *, LAG(enrolled_at) OVER (PARTITION BY course_id ORDER BY enrolled_at, id) AS prev_enrolled_at
    FROM recent_enrollments
),
flagged AS (
    -- Flag rows that start a new session (gap > 2 hours)
    SELECT *, CASE WHEN prev_enrolled_at IS NULL OR enrolled_at > prev_enrolled_at + INTERVAL 2 HOURS THEN 1 ELSE 0 END AS new_session_flag
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(new_session_flag) OVER (PARTITION BY course_id ORDER BY enrolled_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM flagged
),
session_summary AS (
    -- One row per session
    SELECT
        course_id,
        burst_id,
        MIN(enrolled_at) AS session_start,
        MAX(enrolled_at) AS session_end,
        COUNT(*) AS events,
        SUM(score) AS session_score,
        ROUND((UNIX_TIMESTAMP(MAX(enrolled_at)) - UNIX_TIMESTAMP(MIN(enrolled_at))) / 60, 1) AS duration_min
    FROM sessionized
    GROUP BY course_id, burst_id
),
tagged AS (
    -- Label and score rows
    SELECT
        course_id,
        burst_id,
        session_start,
        session_end,
        events,
        session_start + INTERVAL 1 DAYS AS session_start_plus_1d
    FROM session_summary
),
rollup_base AS (
    -- Aggregate with conditional metrics
    SELECT
        events,
        burst_id,
        DATE_TRUNC('MONTH', session_end) AS session_end_month,
        MIN(events) AS min_events,
        SUM(events) AS total_events,
        ROUND(AVG(events), 2) AS avg_events,
        COUNT(DISTINCT course_id) AS distinct_course_ids
    FROM tagged
    GROUP BY events, burst_id, DATE_TRUNC('MONTH', session_end)
)
SELECT
    events,
    burst_id,
    session_end_month,
    min_events,
    total_events,
    avg_events,
    distinct_course_ids,
    burst_id + total_events AS burst_id_plus_total_events,
    TO_DATE(session_end_month) AS session_end_month_date
FROM rollup_base
ORDER BY avg_events
