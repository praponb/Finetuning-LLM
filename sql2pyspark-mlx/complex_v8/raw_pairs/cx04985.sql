WITH base_enrollments AS (
    -- Scope the enrollments rows
    SELECT
        id,
        student_id,
        course_id,
        term,
        grade_status,
        enrolled_at,
        DATE_TRUNC('HOUR', enrolled_at) AS enrolled_at_hour
    FROM enrollments
    WHERE student_id IS NOT NULL
),
with_prev AS (
    -- Previous enrolled_at per grade_status
    SELECT *, LAG(enrolled_at) OVER (PARTITION BY grade_status ORDER BY enrolled_at, id) AS prev_enrolled_at
    FROM base_enrollments
),
flagged AS (
    -- Flag rows that start a new session (gap > 60 minutes)
    SELECT *, CASE WHEN prev_enrolled_at IS NULL OR enrolled_at > prev_enrolled_at + INTERVAL 60 MINUTES THEN 1 ELSE 0 END AS new_session_flag
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(new_session_flag) OVER (PARTITION BY grade_status ORDER BY enrolled_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM flagged
)
SELECT
    id,
    student_id,
    course_id,
    term,
    grade_status,
    enrolled_at,
    enrolled_at_hour,
    prev_enrolled_at,
    session_id,
    COALESCE(grade_status, 'UNKNOWN') AS grade_status_or_unknown
FROM sessionized
WHERE student_id >= 15
ORDER BY student_id, enrolled_at DESC
