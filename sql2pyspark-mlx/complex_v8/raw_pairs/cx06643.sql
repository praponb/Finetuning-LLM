WITH in_window AS (
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
profiled AS (
    -- Attach enrollments metrics
    SELECT
        s.*,
        (SELECT MIN(e.enrolled_at) FROM enrollments e WHERE e.student_id = s.student_id) AS first_enrollment_enrolled_at,
        (SELECT MAX(e.enrolled_at) FROM enrollments e WHERE e.student_id = s.student_id) AS last_enrollment_enrolled_at,
        (SELECT COUNT(*) FROM enrollments e WHERE e.student_id = s.student_id) AS enrollment_count
    FROM in_window s
),
student_id_stats AS (
    -- Median prior_score_3d per student_id
    SELECT student_id, PERCENTILE_APPROX(prior_score_3d, 0.5) AS median_prior_score_3d
    FROM profiled
    GROUP BY student_id
),
vs_group AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.median_prior_score_3d,
        ROUND(s.prior_score_3d - g.median_prior_score_3d, 2) AS prior_score_3d_minus_median
    FROM profiled s
    JOIN student_id_stats g ON g.student_id = s.student_id
    WHERE s.prior_score_3d >= g.median_prior_score_3d
)
SELECT
    id,
    enrolled_at,
    prior_rows_3d,
    prior_score_3d,
    first_enrollment_enrolled_at,
    last_enrollment_enrolled_at,
    enrollment_count,
    median_prior_score_3d,
    prior_score_3d_minus_median,
    ABS(prior_rows_3d - 3) AS prior_rows_3d_distance,
    GREATEST(median_prior_score_3d, enrollment_count) AS greatest_median_prior_score_3d_enrollment_count
FROM vs_group
ORDER BY prior_rows_3d_distance DESC NULLS FIRST
