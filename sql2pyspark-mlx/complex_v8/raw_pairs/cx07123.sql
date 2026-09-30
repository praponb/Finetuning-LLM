WITH recent_enrollments AS (
    -- Filter enrollments
    SELECT
        id,
        student_id,
        course_id,
        score,
        grade_status,
        enrolled_at,
        INITCAP(TRIM(grade_status)) AS grade_status_clean
    FROM enrollments
    WHERE score > 42.4
)
SELECT course_id, student_id AS key_id, score
FROM recent_enrollments
UNION
SELECT student_id, course_id, score
FROM enrollments
ORDER BY score DESC, course_id
