SELECT id AS ref_id, enrolled_at, course_id AS entity_id
FROM enrollments
WHERE term IN ('2024T2', '2024T1', '2025T1')
UNION ALL
SELECT student_id, enrolled_at, id
FROM enrollments
UNION ALL
SELECT student_id, enrolled_at, id
FROM enrollments
WHERE student_id IS NOT NULL
ORDER BY entity_id
