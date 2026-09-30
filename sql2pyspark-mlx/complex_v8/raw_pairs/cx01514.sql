SELECT id AS ref_id
FROM courses
WHERE course_code IS NOT NULL
  AND dept <> 'Art'
EXCEPT ALL
SELECT credits
FROM courses
