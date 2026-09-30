SELECT term AS name, id AS key_id, enrolled_at AS happened_at
FROM enrollments
WHERE term NOT IN ('2024T1', '2025T1')
  AND student_id IS NOT NULL
UNION ALL
SELECT grade_status AS name, id AS key_id, enrolled_at AS happened_at
FROM enrollments
ORDER BY happened_at
