WITH scoped AS (
    -- Scope the enrollments rows
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        enrolled_at
    FROM enrollments
    WHERE score > 5.9
)
SELECT student_id, enrolled_at AS event_time
FROM scoped
UNION
SELECT course_id, enrolled_at
FROM enrollments
ORDER BY student_id, event_time
