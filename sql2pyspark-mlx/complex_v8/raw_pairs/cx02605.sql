WITH unioned AS (
    -- Stack the branches
    SELECT
        cohort,
        student_name AS category,
        enrolled_on AS event_date,
        'student' AS src
    FROM students
    WHERE LOWER(student_name) LIKE '%er%'
      AND major IN ('Math', 'CS')
    UNION
    SELECT
        major,
        student_name,
        enrolled_on,
        'student_2' AS src
    FROM students
    WHERE UPPER(TRIM(student_name)) LIKE 'I%'
)
SELECT src, COUNT(*) AS n_rows, MAX(event_date) AS latest_event_date
FROM unioned
GROUP BY src
ORDER BY src
