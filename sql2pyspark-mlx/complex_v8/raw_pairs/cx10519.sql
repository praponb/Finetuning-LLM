WITH covered AS (
    -- Count earlier rows of the same key inside the look-back window
    SELECT
        a.id,
        a.student_id,
        a.enrolled_at,
        COUNT(b.id) AS prior_rows_1d,
        SUM(b.score) AS prior_score_1d
    FROM enrollments a
    LEFT JOIN enrollments b
      ON b.student_id = a.student_id
     AND b.enrolled_at < a.enrolled_at
     AND b.enrolled_at >= a.enrolled_at - INTERVAL 1 DAYS
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
              AND HOUR(e.enrolled_at) BETWEEN 8 AND 12
        )
)
SELECT
    id,
    student_id,
    enrolled_at,
    prior_rows_1d,
    ROUND(prior_rows_1d / NULLIF(prior_score_1d, 0), 3) AS prior_rows_1d_per_prior_score_1d,
    CASE WHEN prior_score_1d >= 350.0 THEN 'HIGH' WHEN prior_score_1d >= 100.0 THEN 'MEDIUM' ELSE 'LOW' END AS prior_score_1d_tier
FROM with_activity
ORDER BY prior_rows_1d_per_prior_score_1d, id
LIMIT 5
