WITH base_students AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        student_name,
        cohort,
        major,
        enrolled_on,
        gpa,
        ROUND(gpa * 1.5, 2) AS gpa_adj
    FROM students
    WHERE enrolled_on IS NOT NULL
),
combined AS (
    -- Stack the branches
    SELECT 'student' AS record_type, id, student_name AS name
    FROM base_students
    WHERE enrolled_on IS NOT NULL
      AND cohort = 'C2023'
    UNION ALL
    SELECT 'enrollment' AS record_type, id, term AS name
    FROM enrollments
    WHERE student_id IS NOT NULL
)
SELECT record_type, COUNT(*) AS n_rows, SUM(id) AS total_id
FROM combined
GROUP BY record_type
ORDER BY record_type
