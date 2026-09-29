SELECT id AS key_id
FROM enrollments
EXCEPT
SELECT credits AS key_id
FROM courses
WHERE credits BETWEEN 2 AND 4
ORDER BY key_id
