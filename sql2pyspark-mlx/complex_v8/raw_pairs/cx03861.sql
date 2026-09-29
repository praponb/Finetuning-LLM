WITH unioned AS (
    -- Stack the branches
    SELECT 'enrollment' AS source, term AS category, score
    FROM enrollments
    UNION ALL
    SELECT 'enrollment_2' AS source, grade_status, score
    FROM enrollments
)
SELECT source, COUNT(*) AS n_rows, SUM(score) AS total_score
FROM unioned
GROUP BY source
ORDER BY source
