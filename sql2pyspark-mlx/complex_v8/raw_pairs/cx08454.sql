WITH all_events AS (
    -- Stack the branches
    SELECT credits, course_code, 'course' AS source
    FROM courses
    UNION ALL
    SELECT
        student_id AS credits,
        grade_status AS course_code,
        'enrollment' AS source
    FROM enrollments
    WHERE course_id >= 10
    UNION ALL
    SELECT id AS credits, grade_status AS course_code, 'enrollment' AS source
    FROM enrollments
    WHERE student_id >= 11
      AND grade_status = 'PASS'
)
SELECT source, COUNT(*) AS n_rows, SUM(credits) AS total_credits
FROM all_events
GROUP BY source
ORDER BY source
