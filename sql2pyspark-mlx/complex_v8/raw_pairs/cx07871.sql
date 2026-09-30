WITH stacked AS (
    -- Stack the branches
    SELECT course_id, enrolled_at AS event_time
    FROM enrollments
    WHERE course_id IS NOT NULL
      AND student_id >= 19
    UNION ALL
    SELECT student_id, enrolled_at
    FROM enrollments
    WHERE student_id IS NOT NULL
)
SELECT course_id, COUNT(*) AS n_rows, MAX(event_time) AS latest_event_time
FROM stacked
GROUP BY course_id
ORDER BY course_id
