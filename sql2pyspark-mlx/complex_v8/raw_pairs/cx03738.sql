SELECT 'student' AS origin, id AS ref_id, enrolled_on AS day
FROM students
WHERE enrolled_on >= DATE '2024-04-04'
UNION ALL
SELECT 'student_2' AS origin, id, enrolled_on
FROM students
WHERE is_international = TRUE
UNION ALL
SELECT 'student_3' AS origin, id, enrolled_on
FROM students
ORDER BY day, origin
