SELECT
    course_code,
    level AS label,
    id,
    'course' AS origin
FROM courses
WHERE UPPER(TRIM(course_code)) LIKE 'C%'
UNION ALL
SELECT
    grade_status,
    term,
    id,
    'enrollment' AS origin
FROM enrollments
WHERE term = '2024T2'
  AND student_id >= 4
UNION ALL
SELECT
    term,
    grade_status,
    id,
    'enrollment' AS origin
FROM enrollments
WHERE TO_DATE(enrolled_at) < DATE '2025-02-24'
