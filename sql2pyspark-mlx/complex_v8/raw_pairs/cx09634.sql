WITH cleaned AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status
    FROM enrollments
),
with_activity AS (
    -- Rows that have matching enrollments
    SELECT s.*
    FROM cleaned s
    WHERE EXISTS (
            SELECT 1
            FROM enrollments e
            WHERE e.student_id = s.student_id
              AND e.term IS NOT NULL
              AND TO_DATE(e.enrolled_at) < DATE '2025-02-12'
        )
      AND s.course_id <= 4
)
SELECT
    id,
    student_id,
    term,
    score
FROM with_activity
ORDER BY id DESC, student_id
