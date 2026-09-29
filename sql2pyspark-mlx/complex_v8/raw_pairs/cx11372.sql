WITH all_events AS (
    -- Stack the branches
    SELECT credits, course_code AS category, id
    FROM courses
    UNION
    SELECT id, grade_status, student_id
    FROM enrollments
    WHERE enrolled_at >= TIMESTAMP '2025-01-19 11:00:00'
)
SELECT category, COUNT(*) AS n_rows, SUM(id) AS total_id
FROM all_events
GROUP BY category
ORDER BY category
