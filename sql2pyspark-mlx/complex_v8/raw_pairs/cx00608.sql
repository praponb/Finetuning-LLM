WITH scoped AS (
    -- Filter enrollments
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at
    FROM enrollments
    WHERE HOUR(enrolled_at) BETWEEN 9 AND 15
),
with_prev AS (
    -- Previous enrolled_at per term
    SELECT *, LAG(enrolled_at) OVER (PARTITION BY term ORDER BY enrolled_at, id) AS prev_enrolled_at
    FROM scoped
),
flagged AS (
    -- Flag rows that start a new session (gap > 2 hours)
    SELECT *, CASE WHEN prev_enrolled_at IS NULL OR (UNIX_TIMESTAMP(enrolled_at) - UNIX_TIMESTAMP(prev_enrolled_at)) / 60 > 120 THEN 1 ELSE 0 END AS starts_session
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(starts_session) OVER (PARTITION BY term ORDER BY enrolled_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM flagged
)
-- Final projection
SELECT
    id,
    student_id,
    course_id,
    score,
    grade_status,
    enrolled_at,
    prev_enrolled_at,
    starts_session,
    COALESCE(session_no, 0) AS session_no_filled,
    DATEDIFF(DATE '2025-12-31', enrolled_at) AS days_since_enrolled_at
FROM sessionized
ORDER BY student_id
