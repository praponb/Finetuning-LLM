SELECT
    id AS key_id,
    student_id AS key_id_2,
    grade_status AS label,
    'enrollment' AS record_type
FROM enrollments
UNION ALL
SELECT
    student_id AS key_id,
    course_id AS key_id_2,
    grade_status AS label,
    'enrollment_2' AS record_type
FROM enrollments
WHERE score >= 30.0
  AND enrolled_at < TIMESTAMP '2025-03-02 09:00:00'
UNION ALL
SELECT
    id AS key_id,
    credits AS key_id_2,
    level AS label,
    'course' AS record_type
FROM courses
WHERE credits <= 4
ORDER BY label DESC, record_type
