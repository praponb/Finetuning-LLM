WITH stacked AS (
    -- Stack the branches
    SELECT id, gpa, 'student' AS record_type
    FROM students
    WHERE gpa > 2.1
    UNION ALL
    SELECT id, gpa, 'student_2' AS record_type
    FROM students
    WHERE cohort IN ('C2021', 'C2024', 'C2022')
)
SELECT record_type, COUNT(*) AS n_rows, SUM(id) AS total_id
FROM stacked
GROUP BY record_type
ORDER BY record_type
