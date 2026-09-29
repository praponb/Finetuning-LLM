WITH base AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at
    FROM enrollments
    WHERE grade_status NOT IN ('FAIL', 'INCOMPLETE')
)
SELECT
    student_id,
    enrolled_at AS happened_at,
    score,
    'enrollment' AS record_type
FROM base
WHERE student_id <= 3
UNION ALL
SELECT
    course_id AS student_id,
    enrolled_at AS happened_at,
    score,
    'enrollment_2' AS record_type
FROM enrollments
WHERE term IS NOT NULL
  AND student_id <= 9
ORDER BY score, student_id
