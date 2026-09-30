WITH cleaned_enrollments AS (
    -- Filter enrollments
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        enrolled_at
    FROM enrollments
    WHERE grade_status NOT IN ('INCOMPLETE', 'FAIL')
)
SELECT id, enrolled_at
FROM cleaned_enrollments
WHERE term = '2025T1'
  AND course_id IS NOT NULL
UNION ALL
SELECT id, enrolled_at
FROM enrollments
UNION ALL
SELECT student_id, enrolled_at
FROM enrollments
WHERE enrolled_at >= TIMESTAMP '2025-02-06 06:00:00'
