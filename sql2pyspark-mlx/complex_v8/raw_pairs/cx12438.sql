WITH valid_enrollments AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        LENGTH(TRIM(grade_status)) AS grade_status_len
    FROM enrollments
    WHERE grade_status = 'FAIL'
),
screened AS (
    -- Rows with no matching enrollments
    SELECT s.*
    FROM valid_enrollments s
    WHERE NOT EXISTS (
            SELECT 1
            FROM enrollments e
            WHERE e.student_id = s.student_id
              AND e.course_id >= 2
              AND e.term IN ('2024T1', '2025T2')
        )
),
summarized_rows AS (
    -- Attach enrollments metrics
    SELECT
        s.*,
        (SELECT MAX(e.enrolled_at) FROM enrollments e WHERE e.course_id = s.course_id) AS last_enrollment_enrolled_at,
        (SELECT COUNT(*) FROM enrollments e WHERE e.course_id = s.course_id) AS enrollment_count
    FROM screened s
),
with_activity AS (
    -- Rows with no matching enrollments
    SELECT s.*
    FROM summarized_rows s
    WHERE NOT EXISTS (
            SELECT 1
            FROM enrollments e
            WHERE e.course_id = s.course_id
              AND e.grade_status = 'WITHDRAWN'
        )
      AND s.student_id IS NOT NULL
)
-- Final projection
SELECT
    id,
    course_id,
    term,
    score,
    grade_status_len,
    last_enrollment_enrolled_at,
    ROUND(grade_status_len / NULLIF(score, 0), 3) AS grade_status_len_per_score,
    LENGTH(TRIM(grade_status)) AS grade_status_len_2
FROM with_activity
ORDER BY id
