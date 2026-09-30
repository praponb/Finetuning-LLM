WITH scoped AS (
    -- Scope the enrollments rows
    SELECT
        id,
        student_id,
        course_id,
        term,
        grade_status,
        enrolled_at,
        COALESCE(score, 0.0) AS score_filled
    FROM enrollments
    WHERE student_id IS NOT NULL
)
SELECT course_id AS ref_id, score_filled, 'enrollment' AS source
FROM scoped
WHERE term IS NOT NULL
UNION ALL
SELECT id, gpa, 'student' AS source
FROM students
WHERE gpa <= 3.4
UNION ALL
SELECT id, gpa, 'student' AS source
FROM students
ORDER BY source, ref_id
