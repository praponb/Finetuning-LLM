WITH in_window AS (
    -- Count earlier rows of the same key inside the look-back window
    SELECT
        a.id,
        a.student_id,
        a.enrolled_at,
        COUNT(b.id) AS prior_rows_7d,
        SUM(b.score) AS prior_score_7d
    FROM enrollments a
    LEFT JOIN enrollments b
      ON b.student_id = a.student_id
     AND b.enrolled_at < a.enrolled_at
     AND b.enrolled_at >= a.enrolled_at - INTERVAL 7 DAYS
    GROUP BY a.id, a.student_id, a.enrolled_at
),
derived AS (
    -- Derive helper columns
    SELECT
        id,
        student_id,
        enrolled_at,
        prior_rows_7d,
        prior_score_7d,
        enrolled_at + INTERVAL 1 DAYS AS enrolled_at_plus_1d,
        CASE WHEN prior_score_7d >= 350.0 THEN 'A' WHEN prior_score_7d >= 200.0 THEN 'B' ELSE 'C' END AS prior_score_7d_tier,
        DATEDIFF(DATE '2026-01-01', enrolled_at) AS days_since_enrolled_at
    FROM in_window
)
-- Final projection
SELECT
    id,
    prior_rows_7d,
    prior_score_7d,
    prior_score_7d_tier
FROM derived
