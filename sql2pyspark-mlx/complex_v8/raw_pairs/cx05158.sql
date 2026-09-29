WITH scoped AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        enrolled_at
    FROM enrollments
    WHERE grade_status = 'PASS'
),
with_prev AS (
    -- Previous enrolled_at per student_id
    SELECT *, LAG(enrolled_at) OVER (PARTITION BY student_id ORDER BY enrolled_at, id) AS prev_enrolled_at
    FROM scoped
),
flagged AS (
    -- Flag rows that start a new session (gap > 2 hours)
    SELECT *, CASE WHEN prev_enrolled_at IS NULL OR (UNIX_TIMESTAMP(enrolled_at) - UNIX_TIMESTAMP(prev_enrolled_at)) / 60 > 120 THEN 1 ELSE 0 END AS new_session_flag
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(new_session_flag) OVER (PARTITION BY student_id ORDER BY enrolled_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM flagged
),
session_summary AS (
    -- One row per session
    SELECT
        student_id,
        burst_id,
        MIN(enrolled_at) AS session_start,
        MAX(enrolled_at) AS session_end,
        COUNT(*) AS events,
        ROUND((UNIX_TIMESTAMP(MAX(enrolled_at)) - UNIX_TIMESTAMP(MIN(enrolled_at))) / 60, 1) AS duration_min
    FROM sessionized
    GROUP BY student_id, burst_id
),
metrics AS (
    -- Rank and compare rows within groups
    SELECT
        student_id,
        burst_id,
        session_start,
        session_end,
        events,
        duration_min,
        COUNT(*) OVER (PARTITION BY student_id) AS rows_in_student_id,
        LEAD(events, 2) OVER (PARTITION BY student_id ORDER BY session_end DESC, student_id, burst_id) AS next2_events,
        MAX(events) OVER (PARTITION BY student_id) AS student_id_max_events,
        events - LAG(events) OVER w_burst_id_by_duration_min AS events_change
    FROM session_summary
    WINDOW w_student_id_2 AS (PARTITION BY student_id), w_burst_id_by_duration_min AS (PARTITION BY burst_id ORDER BY duration_min, student_id, burst_id)
)
-- Final projection
SELECT
    student_id,
    burst_id,
    session_end,
    duration_min,
    next2_events,
    student_id_max_events,
    events_change
FROM metrics
ORDER BY next2_events, student_id DESC, burst_id
LIMIT 10
