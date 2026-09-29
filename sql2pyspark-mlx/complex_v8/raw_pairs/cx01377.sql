WITH joined_ranges AS (
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
without_matches AS (
    -- Rows that have matching enrollments
    SELECT s.*
    FROM joined_ranges s
    WHERE EXISTS (
            SELECT 1
            FROM enrollments e
            WHERE e.student_id = s.student_id
              AND e.term <> '2024T1'
        )
      AND s.enrolled_at < TIMESTAMP '2025-02-24 12:00:00'
)
-- Final projection
SELECT
    id,
    student_id,
    enrolled_at,
    prior_rows_12h,
    prior_score_12h,
    DATEDIFF(DATE '2026-01-01', enrolled_at) AS days_since_enrolled_at
FROM without_matches
ORDER BY prior_rows_12h DESC, enrolled_at DESC, id
LIMIT 5
