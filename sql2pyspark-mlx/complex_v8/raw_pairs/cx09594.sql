WITH all_events AS (
    -- Stack the branches
    SELECT id, gpa AS amount
    FROM students
    WHERE gpa < 3.4
      AND UPPER(TRIM(student_name)) LIKE 'H%'
    UNION ALL
    SELECT course_id, score
    FROM enrollments
    WHERE student_id >= 11
)
SELECT id, COUNT(*) AS n_rows, SUM(amount) AS total_amount
FROM all_events
GROUP BY id
ORDER BY id
