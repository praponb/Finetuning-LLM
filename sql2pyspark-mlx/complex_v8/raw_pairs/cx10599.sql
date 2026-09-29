WITH joined_ranges AS (
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
qualified AS (
    -- Rows with no matching enrollments
    SELECT s.*
    FROM joined_ranges s
    WHERE NOT EXISTS (
            SELECT 1
            FROM enrollments e
            WHERE e.student_id = s.student_id
              AND TO_DATE(e.enrolled_at) < DATE '2025-02-24'
              AND e.grade_status IS NOT NULL
        )
)
-- Final projection
SELECT
    id,
    enrolled_at,
    prior_rows_3d,
    prior_score_3d,
    DATEDIFF(DATE '2026-01-01', enrolled_at) AS days_since_enrolled_at,
    ABS(prior_rows_3d - 4) AS prior_rows_3d_distance
FROM qualified
ORDER BY days_since_enrolled_at, prior_rows_3d NULLS LAST
