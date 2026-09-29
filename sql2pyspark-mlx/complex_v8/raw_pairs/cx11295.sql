WITH recent AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        student_name,
        cohort,
        gpa
    FROM students
    WHERE enrolled_on >= DATE '2024-03-15'
)
SELECT id, enrolled_on, 'student' AS record_type
FROM students
WHERE gpa < 3.4
UNION ALL
SELECT id, enrolled_on, 'student_2' AS record_type
FROM students
WHERE gpa > 2.5
  AND major = 'Bio'
UNION ALL
SELECT id, enrolled_on, 'student_3' AS record_type
FROM students
WHERE major = 'Art'
  AND enrolled_on >= DATE '2024-03-15'
ORDER BY record_type, enrolled_on
