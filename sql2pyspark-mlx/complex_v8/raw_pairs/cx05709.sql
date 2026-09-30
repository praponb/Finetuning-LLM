SELECT id AS ref_id, cohort AS category
FROM students
INTERSECT
SELECT id AS ref_id, cohort AS category
FROM students
ORDER BY category DESC
