SELECT id AS entity_id, enrolled_at
FROM enrollments
WHERE term IN ('2024T2', '2025T1')
UNION ALL
SELECT id, enrolled_at
FROM enrollments
WHERE grade_status IS NOT NULL
  AND term NOT IN ('2025T1', '2024T1')
UNION ALL
SELECT id, enrolled_at
FROM enrollments
WHERE enrolled_at >= TIMESTAMP '2025-01-25 14:00:00'
