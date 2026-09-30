WITH unioned AS (
    -- Stack the branches
    SELECT 'course' AS src, credits, id
    FROM courses
    UNION ALL
    SELECT 'enrollment' AS src, id, student_id
    FROM enrollments
    WHERE score >= 10.8
      AND student_id IS NOT NULL
)
SELECT src, COUNT(*) AS n_rows, SUM(id) AS total_id
FROM unioned
GROUP BY src
ORDER BY src
