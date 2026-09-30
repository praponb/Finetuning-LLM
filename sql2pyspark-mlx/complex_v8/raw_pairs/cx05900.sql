WITH combined AS (
    -- Stack the branches
    SELECT
        id,
        gpa,
        enrolled_on AS day,
        'student' AS record_type
    FROM students
    WHERE is_international = TRUE
      AND enrolled_on IS NOT NULL
    UNION ALL
    SELECT
        id,
        gpa,
        enrolled_on,
        'student_2' AS record_type
    FROM students
)
SELECT record_type, COUNT(*) AS n_rows, SUM(gpa) AS total_gpa, MAX(day) AS latest_day
FROM combined
GROUP BY record_type
ORDER BY record_type
