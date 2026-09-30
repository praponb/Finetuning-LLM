WITH filtered_enrollments AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        student_id,
        course_id,
        score,
        grade_status,
        enrolled_at,
        CASE WHEN grade_status IN ('PASS', 'INCOMPLETE') THEN 'group_a' WHEN grade_status = 'FAIL' THEN 'group_b' ELSE 'other' END AS grade_status_group
    FROM enrollments
    WHERE student_id IS NOT NULL
)
SELECT grade_status_group AS label
FROM filtered_enrollments
WHERE student_id >= 6
MINUS
SELECT term
FROM enrollments
WHERE course_id >= 9
