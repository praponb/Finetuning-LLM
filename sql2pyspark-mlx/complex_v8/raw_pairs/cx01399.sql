WITH all_events AS (
    -- Stack the branches
    SELECT student_name, gpa
    FROM students
    UNION ALL
    SELECT student_name, gpa
    FROM students
    WHERE gpa BETWEEN 1.7 AND 3.4
      AND is_international = TRUE
)
SELECT student_name, COUNT(*) AS n_rows, SUM(gpa) AS total_gpa
FROM all_events
GROUP BY student_name
ORDER BY student_name
