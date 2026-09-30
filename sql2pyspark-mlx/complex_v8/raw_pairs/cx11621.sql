WITH cleaned_enrollments AS (
    -- Filter enrollments
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status
    FROM enrollments
)
-- Final projection
SELECT
    id,
    course_id,
    score,
    grade_status
FROM cleaned_enrollments
WHERE term NOT IN ('2024T1', '2025T1')
ORDER BY id NULLS LAST
LIMIT 10
