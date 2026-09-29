WITH unioned AS (
    -- Stack the branches
    SELECT 'student' AS source, id, student_name
    FROM students
    UNION ALL
    SELECT 'course' AS source, id, level
    FROM courses
    UNION ALL
    SELECT 'student_3' AS source, id, major
    FROM students
    WHERE cohort = 'C2021'
      AND LENGTH(TRIM(student_name)) >= 7
)
SELECT source, COUNT(*) AS n_rows, SUM(id) AS total_id
FROM unioned
GROUP BY source
ORDER BY source
