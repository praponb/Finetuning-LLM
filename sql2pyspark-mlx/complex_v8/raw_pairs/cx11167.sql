WITH scoped_students AS (
    -- Filter students
    SELECT
        id,
        student_name,
        cohort,
        major
    FROM students
    WHERE is_international = TRUE
      AND major NOT IN ('Bio', 'Math')
),
all_events AS (
    -- Stack the branches
    SELECT id, enrolled_on, 'student' AS record_type
    FROM students
    WHERE NOT is_international
      AND enrolled_on < DATE '2025-03-20'
    UNION
    SELECT id, enrolled_on, 'student_2' AS record_type
    FROM students
)
SELECT record_type, COUNT(*) AS n_rows, SUM(id) AS total_id, MAX(enrolled_on) AS latest_enrolled_on
FROM all_events
GROUP BY record_type
ORDER BY record_type
