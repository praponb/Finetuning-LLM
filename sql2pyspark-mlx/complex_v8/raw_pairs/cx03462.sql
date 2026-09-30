WITH filtered AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        DATE_TRUNC('YEAR', enrolled_at) AS enrolled_at_year
    FROM enrollments
    WHERE grade_status <> 'FAIL'
),
all_events AS (
    -- Stack the branches
    SELECT grade_status, student_id AS qty
    FROM filtered
    UNION ALL
    SELECT dept, credits
    FROM courses
)
SELECT grade_status, COUNT(*) AS n_rows, SUM(qty) AS total_qty
FROM all_events
GROUP BY grade_status
ORDER BY grade_status
