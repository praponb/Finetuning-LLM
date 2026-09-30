WITH stacked AS (
    -- Stack the branches
    SELECT id, gpa AS amount
    FROM students
    WHERE cohort IN ('C2022', 'C2023')
      AND NOT is_international
    UNION ALL
    SELECT student_id, score
    FROM enrollments
)
SELECT id, COUNT(*) AS n_rows, SUM(amount) AS total_amount
FROM stacked
GROUP BY id
ORDER BY id
