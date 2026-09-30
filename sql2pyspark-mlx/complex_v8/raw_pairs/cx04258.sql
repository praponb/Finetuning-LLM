WITH unioned AS (
    -- Stack the branches
    SELECT grade_status, student_id AS ref_id, 'enrollment' AS source
    FROM enrollments
    UNION ALL
    SELECT level, credits, 'course' AS source
    FROM courses
    WHERE dept = 'Art'
)
SELECT source, COUNT(*) AS n_rows, SUM(ref_id) AS total_ref_id
FROM unioned
GROUP BY source
ORDER BY source
