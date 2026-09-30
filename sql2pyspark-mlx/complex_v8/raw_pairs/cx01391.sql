WITH covered AS (
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
with_counts AS (
    -- Attach enrollments metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FROM enrollments e WHERE e.student_id = s.student_id) AS enrollment_count,
        (SELECT MIN(e.score) FROM enrollments e WHERE e.student_id = s.student_id) AS min_enrollment_score
    FROM covered s
),
enriched AS (
    -- Label and score rows
    SELECT
        *,
        FLOOR(min_enrollment_score / 25) * 25 AS min_enrollment_score_bucket,
        LEAST(prior_rows_3d, prior_score_3d) AS least_prior_rows_3d_prior_score_3d,
        ROUND(prior_score_3d * 1.5, 2) AS prior_score_3d_adj
    FROM with_counts
)
SELECT
    id,
    prior_score_3d,
    enrollment_count,
    min_enrollment_score,
    min_enrollment_score_bucket,
    least_prior_rows_3d_prior_score_3d,
    prior_score_3d_adj,
    ROUND(prior_score_3d_adj * 1.5, 2) AS prior_score_3d_adj_adj,
    enrolled_at + INTERVAL 1 DAYS AS enrolled_at_plus_1d
FROM enriched
WHERE prior_rows_3d > 4
