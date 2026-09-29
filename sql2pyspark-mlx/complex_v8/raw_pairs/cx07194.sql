WITH all_events AS (
    -- Stack the branches
    SELECT student_id AS qty, enrolled_at
    FROM enrollments
    WHERE term IS NOT NULL
      AND enrolled_at >= TIMESTAMP '2025-01-07 11:00:00'
    UNION ALL
    SELECT student_id, enrolled_at
    FROM enrollments
    WHERE grade_status = 'FAIL'
)
SELECT qty, COUNT(*) AS n_rows, MAX(enrolled_at) AS latest_enrolled_at
FROM all_events
GROUP BY qty
ORDER BY qty
