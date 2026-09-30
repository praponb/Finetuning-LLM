SELECT credits AS key_id, id AS ref_id, 'course' AS source
FROM courses
WHERE level <> 'advanced'
UNION ALL
SELECT id AS key_id, credits AS ref_id, 'course_2' AS source
FROM courses
ORDER BY source
