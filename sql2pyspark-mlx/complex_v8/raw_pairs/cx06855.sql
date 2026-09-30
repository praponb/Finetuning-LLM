WITH covered AS (
    -- Count earlier rows of the same key inside the look-back window
    SELECT
        a.id,
        a.student_id,
        a.enrolled_at,
        COUNT(b.id) AS prior_rows_12h,
        SUM(b.score) AS prior_score_12h
    FROM enrollments a
    LEFT JOIN enrollments b
      ON b.student_id = a.student_id
     AND b.enrolled_at < a.enrolled_at
     AND b.enrolled_at >= a.enrolled_at - INTERVAL 12 HOURS
    GROUP BY a.id, a.student_id, a.enrolled_at
),
with_activity AS (
    -- Rows that have matching enrollments
    SELECT s.*
    FROM covered s
    WHERE s.student_id IN (
            SELECT e.student_id
            FROM enrollments e
            WHERE e.course_id <= 5
        )
      AND s.student_id IS NOT NULL
)
SELECT id, enrolled_at, prior_score_12h
FROM with_activity
ORDER BY prior_score_12h DESC, id
