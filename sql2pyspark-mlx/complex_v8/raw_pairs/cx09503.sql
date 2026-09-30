WITH all_events AS (
    -- Stack the branches
    SELECT id, cohort AS category
    FROM students
    WHERE cohort IN ('C2021', 'C2024', 'C2023')
      AND gpa <= 3.3
    UNION ALL
    SELECT credits, dept
    FROM courses
    WHERE UPPER(TRIM(course_code)) LIKE 'C%'
      AND dept = 'Art'
)
SELECT category, COUNT(*) AS n_rows, SUM(id) AS total_id
FROM all_events
GROUP BY category
ORDER BY category
