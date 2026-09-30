WITH cleaned_enrollments AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        enrolled_at,
        enrolled_at + INTERVAL 2 HOURS AS enrolled_at_plus_2h
    FROM enrollments
    WHERE score >= 10.8
)
SELECT student_id AS entity_id
FROM cleaned_enrollments
WHERE term = '2025T2'
EXCEPT
SELECT course_id
FROM enrollments
ORDER BY entity_id DESC
