WITH combined AS (
    -- Stack the branches
    SELECT id, gpa AS metric, enrolled_on
    FROM students
    WHERE cohort <> 'C2022'
    UNION
    SELECT id, gpa, enrolled_on
    FROM students
)
SELECT id, COUNT(*) AS n_rows, SUM(metric) AS total_metric, MAX(enrolled_on) AS latest_enrolled_on
FROM combined
GROUP BY id
ORDER BY id
