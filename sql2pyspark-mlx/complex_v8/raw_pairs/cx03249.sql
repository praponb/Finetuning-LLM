SELECT term AS label, id, 'enrollment' AS src
FROM enrollments
WHERE grade_status IS NOT NULL
  AND student_id <= 4
UNION ALL
SELECT level, id, 'course' AS src
FROM courses
WHERE credits < 4
  AND dept <> 'Math'
UNION ALL
SELECT grade_status, student_id, 'enrollment_3' AS src
FROM enrollments
ORDER BY label
