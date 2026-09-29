SELECT 'enrollment' AS record_type, student_id AS key_id, enrolled_at
FROM enrollments
WHERE student_id IS NOT NULL
  AND course_id <= 6
UNION ALL
SELECT 'enrollment_2' AS record_type, id AS key_id, enrolled_at
FROM enrollments
WHERE score > 5.9
  AND grade_status = 'PASS'
