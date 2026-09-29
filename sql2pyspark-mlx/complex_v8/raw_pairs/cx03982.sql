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
derived AS (
    -- Add calculated columns
    SELECT
        *,
        LEAST(prior_rows_1d, prior_score_1d) AS least_prior_rows_1d_prior_score_1d,
        CASE WHEN prior_score_1d >= 400.0 THEN 'large' WHEN prior_score_1d >= 225.0 THEN 'medium' ELSE 'small' END AS prior_score_1d_tier,
        ROUND(prior_score_1d * 1.5, 2) AS prior_score_1d_adj
    FROM covered
)
SELECT
    id,
    prior_rows_1d,
    prior_score_1d,
    least_prior_rows_1d_prior_score_1d,
    prior_score_1d_tier,
    prior_score_1d_adj,
    LEAST(prior_score_1d_adj, prior_rows_1d) AS least_prior_score_1d_adj_prior_rows_1d,
    COALESCE(prior_score_1d_tier, 'UNKNOWN') AS prior_score_1d_tier_or_unknown
FROM derived
WHERE prior_score_1d_adj >= 262.5
ORDER BY least_prior_rows_1d_prior_score_1d, prior_score_1d_tier
