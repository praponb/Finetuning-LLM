WITH valid_enrollments AS (
    -- Keep valid rows and derive helper columns
    SELECT *
    FROM enrollments
),
with_prev AS (
    -- Previous enrolled_at per student_id
    SELECT *, LAG(enrolled_at) OVER (PARTITION BY student_id ORDER BY enrolled_at, id) AS prev_enrolled_at
    FROM valid_enrollments
),
flagged AS (
    -- Flag rows that start a new session (gap > 45 minutes)
    SELECT *, CASE WHEN prev_enrolled_at IS NULL OR enrolled_at > prev_enrolled_at + INTERVAL 45 MINUTES THEN 1 ELSE 0 END AS starts_session
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(starts_session) OVER (PARTITION BY student_id ORDER BY enrolled_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM flagged
)
SELECT *
FROM (
    SELECT enrolled_at, term, grade_status, session_id
    FROM sessionized
) src
PIVOT (
    MAX(session_id) AS max_v, COUNT(session_id) AS n
    FOR grade_status IN ('WITHDRAWN', 'FAIL', 'INCOMPLETE', 'PASS')
)
