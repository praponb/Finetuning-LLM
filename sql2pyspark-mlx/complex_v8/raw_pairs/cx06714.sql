WITH base_students AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        student_name,
        enrolled_on,
        gpa
    FROM students
    WHERE is_international = FALSE
      AND cohort <> 'C2021'
),
combined AS (
    -- Stack the branches
    SELECT student_name AS category, gpa
    FROM base_students
    WHERE enrolled_on BETWEEN DATE '2024-04-04' AND DATE '2025-08-27'
    UNION
    SELECT term AS category, score AS gpa
    FROM enrollments
)
SELECT category, COUNT(*) AS n_rows, SUM(gpa) AS total_gpa
FROM combined
GROUP BY category
ORDER BY category
