WITH prepared_enrollments AS (
    -- Filter enrollments
    SELECT
        id,
        student_id,
        course_id,
        score,
        grade_status,
        enrolled_at,
        DATE_TRUNC('DAY', enrolled_at) AS enrolled_at_day
    FROM enrollments
),
with_prev AS (
    -- Previous enrolled_at per student_id
    SELECT *, LAG(enrolled_at) OVER (PARTITION BY student_id ORDER BY enrolled_at, id) AS prev_enrolled_at
    FROM prepared_enrollments
),
flagged AS (
    -- Flag rows that start a new session (gap > 90 minutes)
    SELECT *, CASE WHEN prev_enrolled_at IS NULL OR enrolled_at > prev_enrolled_at + INTERVAL 90 MINUTES THEN 1 ELSE 0 END AS starts_session
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(starts_session) OVER (PARTITION BY student_id ORDER BY enrolled_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
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
        SUM(score) AS session_score,
        ROUND((UNIX_TIMESTAMP(MAX(enrolled_at)) - UNIX_TIMESTAMP(MIN(enrolled_at))) / 60, 1) AS duration_min
    FROM sessionized
    GROUP BY student_id, burst_id
)
SELECT
    student_id,
    burst_id,
    events,
    duration_min,
    ROUND((UNIX_TIMESTAMP(session_end) - UNIX_TIMESTAMP(session_start)) / 3600, 1) AS hours_session_start_to_session_end
FROM session_summary
WHERE session_end >= TIMESTAMP '2025-01-13 09:00:00'
ORDER BY duration_min DESC NULLS FIRST, student_id, burst_id
LIMIT 10
