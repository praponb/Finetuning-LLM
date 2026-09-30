WITH scoped AS (
    -- Scope the enrollments rows
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at
    FROM enrollments
    WHERE score <= 63.9
)
SELECT course_id AS qty, term AS name, enrolled_at
FROM scoped
WHERE score >= 32.7
  AND course_id <= 8
UNION ALL
SELECT student_id, term, enrolled_at
FROM enrollments
WHERE term IN ('2024T2', '2025T1', '2024T1')
UNION ALL
SELECT student_id, term, enrolled_at
FROM enrollments
