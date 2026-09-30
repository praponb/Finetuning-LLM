SELECT
    'student' AS src,
    id AS key_id,
    cohort,
    enrolled_on AS event_date
FROM students
WHERE enrolled_on >= DATE '2024-10-09'
  AND major <> 'Math'
UNION
SELECT
    'student_2' AS src,
    id AS key_id,
    cohort,
    enrolled_on AS event_date
FROM students
