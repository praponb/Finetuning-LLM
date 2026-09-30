SELECT credits AS ref_id
FROM courses
EXCEPT
SELECT id
FROM courses
WHERE dept IN ('Art', 'Bio', 'Biz')
ORDER BY ref_id
