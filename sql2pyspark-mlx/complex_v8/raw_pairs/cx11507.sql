SELECT course_id AS entity_id, id AS entity_id_2, 'enrollment' AS src
FROM enrollments
WHERE course_id IS NOT NULL
  AND term NOT IN ('2024T1', '2025T2')
UNION ALL
SELECT credits AS entity_id, id AS entity_id_2, 'course' AS src
FROM courses
WHERE dept NOT IN ('Art', 'CS')
  AND level IN ('advanced', 'intro')
UNION ALL
SELECT id AS entity_id, credits AS entity_id_2, 'course' AS src
FROM courses
ORDER BY entity_id DESC, entity_id_2
