WITH all_events AS (
    -- Stack the branches
    SELECT
        'student' AS src,
        id,
        gpa,
        enrolled_on AS event_date
    FROM students
    UNION ALL
    SELECT
        'student_2' AS src,
        id,
        gpa,
        enrolled_on AS event_date
    FROM students
)
SELECT src, COUNT(*) AS n_rows, SUM(gpa) AS total_gpa, MAX(event_date) AS latest_event_date
FROM all_events
GROUP BY src
ORDER BY src
