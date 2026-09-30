WITH matched AS (
    -- Count earlier rows of the same key inside the look-back window
    SELECT
        a.id,
        a.student_id,
        a.enrolled_at,
        COUNT(b.id) AS prior_rows_3d,
        SUM(b.score) AS prior_score_3d
    FROM enrollments a
    LEFT JOIN enrollments b
      ON b.student_id = a.student_id
     AND b.enrolled_at < a.enrolled_at
     AND b.enrolled_at >= a.enrolled_at - INTERVAL 3 DAYS
    GROUP BY a.id, a.student_id, a.enrolled_at
),
with_activity AS (
    -- Rows with no matching enrollments
    SELECT s.*
    FROM matched s
    WHERE NOT EXISTS (
            SELECT 1
            FROM enrollments e
            WHERE e.student_id = s.student_id
              AND e.score BETWEEN 4.9 AND 85.8
        )
),
annotated AS (
    -- Label and score rows
    SELECT
        id,
        student_id,
        enrolled_at,
        prior_score_3d,
        DATE_TRUNC('DAY', enrolled_at) AS enrolled_at_day
    FROM with_activity
)
-- Final projection
SELECT
    id,
    prior_score_3d,
    enrolled_at_day,
    YEAR(enrolled_at_day) AS enrolled_at_day_year
FROM annotated
WHERE student_id IS NOT NULL
ORDER BY enrolled_at_day_year NULLS LAST, prior_score_3d DESC
