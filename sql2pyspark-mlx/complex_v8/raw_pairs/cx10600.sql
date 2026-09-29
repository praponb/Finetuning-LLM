WITH matched AS (
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
summarized_rows AS (
    -- Attach enrollments metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FROM enrollments e WHERE e.student_id = s.student_id) AS enrollment_count,
        (SELECT COUNT(*) FILTER (WHERE e.term NOT IN ('2025T2', '2024T1')) FROM enrollments e WHERE e.student_id = s.student_id) AS enrollment_term_count,
        (SELECT COUNT(*) FROM enrollments e WHERE e.student_id = s.student_id) AS enrollment_count_2
    FROM matched s
)
-- Final projection
SELECT
    id,
    enrolled_at,
    prior_rows_7d,
    enrollment_count,
    enrollment_term_count,
    enrollment_count_2,
    DATEDIFF(DATE '2025-06-30', enrolled_at) AS days_since_enrolled_at
FROM summarized_rows
WHERE prior_score_7d <= 450.0
ORDER BY enrolled_at DESC
