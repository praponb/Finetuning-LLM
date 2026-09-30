WITH valid_enrollments AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        student_id,
        course_id,
        score,
        grade_status,
        enrolled_at
    FROM enrollments
),
with_prev AS (
    -- Previous enrolled_at per student_id
    SELECT *, LAG(enrolled_at) OVER (PARTITION BY student_id ORDER BY enrolled_at, id) AS prev_enrolled_at
    FROM valid_enrollments
),
flagged AS (
    -- Flag rows that start a new session (gap > 45 minutes)
    SELECT *, CASE WHEN prev_enrolled_at IS NULL OR enrolled_at > prev_enrolled_at + INTERVAL 45 MINUTES THEN 1 ELSE 0 END AS new_session_flag
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(new_session_flag) OVER (PARTITION BY student_id ORDER BY enrolled_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM flagged
),
enriched AS (
    -- Derive helper columns
    SELECT
        *,
        CASE WHEN session_no >= 4 THEN 'HIGH' WHEN session_no >= 2 THEN 'MEDIUM' ELSE 'LOW' END AS session_no_tier,
        enrolled_at + INTERVAL 2 HOURS AS enrolled_at_plus_2h
    FROM sessionized
)
SELECT
    id,
    course_id,
    new_session_flag,
    session_no,
    session_no_tier,
    enrolled_at_plus_2h
FROM enriched
WHERE enrolled_at < TIMESTAMP '2025-03-08 08:00:00'
ORDER BY id DESC, session_no
