WITH all_events AS (
    -- Stack the branches
    SELECT id AS qty, credits
    FROM courses
    WHERE UPPER(TRIM(course_code)) LIKE 'B%'
    UNION ALL
    SELECT course_id, student_id
    FROM enrollments
    UNION ALL
    SELECT id, student_id
    FROM enrollments
    WHERE course_id <= 10
      AND term IN ('2024T1', '2025T1')
)
SELECT qty, COUNT(*) AS n_rows, SUM(credits) AS total_credits
FROM all_events
GROUP BY qty
ORDER BY qty
