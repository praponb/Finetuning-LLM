WITH combined AS (
    -- Stack the branches
    SELECT id AS entity_id, enrolled_on, 'student' AS origin
    FROM students
    WHERE cohort = 'C2024'
      AND LENGTH(TRIM(student_name)) >= 5
    UNION ALL
    SELECT id, enrolled_on, 'student_2' AS origin
    FROM students
    WHERE major <> 'Bio'
    UNION ALL
    SELECT id, enrolled_on, 'student_3' AS origin
    FROM students
    WHERE enrolled_on BETWEEN DATE '2024-04-04' AND DATE '2025-08-27'
      AND major <> 'CS'
)
SELECT origin, COUNT(*) AS n_rows, SUM(entity_id) AS total_entity_id, MAX(enrolled_on) AS latest_enrolled_on
FROM combined
GROUP BY origin
ORDER BY origin
