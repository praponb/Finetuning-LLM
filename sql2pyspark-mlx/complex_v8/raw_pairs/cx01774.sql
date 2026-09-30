SELECT cohort AS label, gpa AS amount
FROM students
WHERE major IN ('Art', 'Biz')
UNION ALL
SELECT term, score
FROM enrollments
WHERE term IS NOT NULL
ORDER BY amount, label
