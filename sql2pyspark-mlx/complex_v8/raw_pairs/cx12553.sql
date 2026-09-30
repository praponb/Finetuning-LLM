WITH all_events AS (
    -- Stack the branches
    SELECT id, gpa AS amount
    FROM students
    WHERE major = 'Math'
    UNION
    SELECT course_id, score
    FROM enrollments
)
SELECT id, COUNT(*) AS n_rows, SUM(amount) AS total_amount
FROM all_events
GROUP BY id
ORDER BY id
