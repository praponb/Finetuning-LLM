SELECT
    term AS category,
    enrolled_at,
    grade_status AS label,
    'enrollment' AS record_type
FROM enrollments
WHERE student_id IS NOT NULL
  AND term NOT IN ('2025T1', '2025T2')
UNION ALL
SELECT
    grade_status,
    enrolled_at,
    term,
    'enrollment_2' AS record_type
FROM enrollments
WHERE student_id IS NOT NULL
  AND TO_DATE(enrolled_at) < DATE '2025-02-24'
UNION ALL
SELECT
    term,
    enrolled_at,
    grade_status,
    'enrollment_3' AS record_type
FROM enrollments
WHERE student_id IS NOT NULL
ORDER BY label DESC
