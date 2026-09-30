WITH filtered_enrollments AS (
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
    -- Previous enrolled_at per grade_status
    SELECT *, LAG(enrolled_at) OVER (PARTITION BY grade_status ORDER BY enrolled_at, id) AS prev_enrolled_at
    FROM filtered_enrollments
),
flagged AS (
    -- Flag rows that start a new session (gap > 90 minutes)
    SELECT *, CASE WHEN prev_enrolled_at IS NULL OR (UNIX_TIMESTAMP(enrolled_at) - UNIX_TIMESTAMP(prev_enrolled_at)) / 60 > 90 THEN 1 ELSE 0 END AS starts_session
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(starts_session) OVER (PARTITION BY grade_status ORDER BY enrolled_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM flagged
),
ranked AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        LEAD(score) OVER w_all_by_score AS next_score,
        FIRST_VALUE(starts_session) IGNORE NULLS OVER (ORDER BY score ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_starts_session,
        ROW_NUMBER() OVER w_all_by_score AS seq_no,
        SUM(starts_session) OVER (ORDER BY score ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_starts_session,
        COUNT(*) OVER (PARTITION BY session_no, student_id) AS rows_in_session_no_student_id,
        SIZE(COLLECT_SET(grade_status) OVER (PARTITION BY session_no, student_id)) AS distinct_grade_status_in_session_no_student_id
    FROM sessionized
    WINDOW w_all_by_score AS (ORDER BY score ASC, id)
),
with_activity AS (
    -- Rows with no matching enrollments
    SELECT s.*
    FROM ranked s
    WHERE NOT EXISTS (
            SELECT 1
            FROM enrollments e
            WHERE e.course_id = s.course_id
              AND MONTH(e.enrolled_at) IN (2, 3)
              AND e.student_id <= 5
        )
)
-- Final projection
SELECT
    id,
    student_id,
    course_id,
    score,
    prev_enrolled_at,
    next_score,
    first_known_starts_session,
    running_starts_session,
    rows_in_session_no_student_id,
    distinct_grade_status_in_session_no_student_id
FROM with_activity
ORDER BY id, course_id NULLS LAST
LIMIT 5
