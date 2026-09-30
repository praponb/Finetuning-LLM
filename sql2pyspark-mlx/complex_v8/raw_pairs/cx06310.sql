WITH valid_enrollments AS (
    -- Scope the enrollments rows
    SELECT
        id,
        student_id,
        course_id,
        term,
        grade_status,
        enrolled_at,
        LENGTH(TRIM(term)) AS term_len
    FROM enrollments
    WHERE score > 13.9
)
SELECT student_id, term AS label, 'enrollment' AS origin
FROM valid_enrollments
WHERE course_id IS NOT NULL
UNION ALL
SELECT id AS student_id, major AS label, 'student' AS origin
FROM students
UNION ALL
SELECT credits AS student_id, course_code AS label, 'course' AS origin
FROM courses
WHERE level IN ('advanced', 'intro')
  AND LOWER(course_code) LIKE '%01%'
ORDER BY origin DESC
