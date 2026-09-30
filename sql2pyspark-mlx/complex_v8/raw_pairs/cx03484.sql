WITH prepared_enrollments AS (
    -- Filter enrollments
    SELECT
        id,
        student_id,
        course_id,
        term,
        grade_status,
        enrolled_at,
        CASE WHEN score >= 76.8 THEN 'HIGH' WHEN score >= 39.4 THEN 'MEDIUM' ELSE 'LOW' END AS score_tier,
        FLOOR(score / 25) * 25 AS score_bucket,
        COALESCE(score, 0.0) AS score_filled
    FROM enrollments
    WHERE grade_status <> 'WITHDRAWN'
      AND term = '2024T1'
),
grouped AS (
    -- Group-level totals
    SELECT
        term,
        ROUND(AVG(score_bucket), 2) AS avg_score_bucket,
        COUNT(*) AS row_count,
        COUNT(*) FILTER (WHERE term = '2024T1') AS n_term_2024t1
    FROM prepared_enrollments
    GROUP BY term
),
without_matches AS (
    -- Compare against the overall avg
    SELECT *
    FROM grouped
    WHERE n_term_2024t1 > (SELECT AVG(n_term_2024t1) FROM grouped)
)
SELECT
    term,
    avg_score_bucket,
    n_term_2024t1 - avg_score_bucket AS n_term_2024t1_minus_avg_score_bucket,
    CASE WHEN row_count >= 8 THEN 'large' WHEN row_count >= 4 THEN 'medium' ELSE 'small' END AS row_count_tier
FROM without_matches
WHERE avg_score_bucket < 60.0
ORDER BY row_count_tier DESC, term
