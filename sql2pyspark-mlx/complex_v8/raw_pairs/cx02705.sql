WITH stacked AS (
    -- Stack the branches
    SELECT grade_status, score, id
    FROM enrollments
    UNION
    SELECT major, gpa, id
    FROM students
)
SELECT grade_status, COUNT(*) AS n_rows, SUM(id) AS total_id
FROM stacked
GROUP BY grade_status
ORDER BY grade_status
