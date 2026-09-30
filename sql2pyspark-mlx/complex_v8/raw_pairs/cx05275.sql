SELECT
    course_id AS ref_id,
    grade_status,
    id,
    'enrollment' AS src
FROM enrollments
UNION ALL
SELECT
    id AS ref_id,
    course_code AS grade_status,
    credits AS id,
    'course' AS src
FROM courses
UNION ALL
SELECT
    course_id AS ref_id,
    grade_status,
    student_id AS id,
    'enrollment_3' AS src
FROM enrollments
WHERE term = '2024T1'
