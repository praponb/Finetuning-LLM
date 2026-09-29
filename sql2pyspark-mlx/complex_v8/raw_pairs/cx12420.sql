WITH combined AS (
    -- Stack the branches
    SELECT id AS key_id, grade_status, student_id AS ref_id
    FROM enrollments
    UNION ALL
    SELECT id AS key_id, term AS grade_status, student_id AS ref_id
    FROM enrollments
    UNION ALL
    SELECT credits AS key_id, dept AS grade_status, id AS ref_id
    FROM courses
    WHERE level = 'advanced'
      AND dept = 'Bio'
)
SELECT grade_status, COUNT(*) AS n_rows, SUM(ref_id) AS total_ref_id
FROM combined
GROUP BY grade_status
ORDER BY grade_status
