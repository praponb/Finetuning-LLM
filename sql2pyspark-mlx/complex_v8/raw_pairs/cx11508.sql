WITH cleaned_enrollments AS (
    -- Scope the enrollments rows
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at
    FROM enrollments
),
with_prev AS (
    -- Previous enrolled_at per student_id
    SELECT *, LAG(enrolled_at) OVER (PARTITION BY student_id ORDER BY enrolled_at, id) AS prev_enrolled_at
    FROM cleaned_enrollments
),
flagged AS (
    -- Flag rows that start a new session (gap > 60 minutes)
    SELECT *, CASE WHEN prev_enrolled_at IS NULL OR enrolled_at > prev_enrolled_at + INTERVAL 60 MINUTES THEN 1 ELSE 0 END AS starts_session
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(starts_session) OVER (PARTITION BY student_id ORDER BY enrolled_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM flagged
)
-- Final projection
SELECT
    id,
    student_id,
    course_id,
    term,
    score,
    enrolled_at,
    prev_enrolled_at,
    starts_session,
    session_no,
    LEAST(session_no, starts_session) AS least_session_no_starts_session,
    ROUND(score * 0.25, 2) AS score_adj
FROM sessionized
ORDER BY enrolled_at DESC NULLS FIRST, least_session_no_starts_session DESC, id
LIMIT 10
