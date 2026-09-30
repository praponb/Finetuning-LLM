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
with_stats AS (
    -- Attach enrollments metrics
    SELECT
        s.*,
        (SELECT MIN(e.enrolled_at) FROM enrollments e WHERE e.student_id = s.student_id) AS first_enrollment_enrolled_at
    FROM in_window s
),
summarized_rows AS (
    -- Attach enrollments metrics
    SELECT
        s.*,
        (SELECT MAX(e.enrolled_at) FROM enrollments e WHERE e.student_id = s.student_id) AS last_enrollment_enrolled_at,
        (SELECT MIN(e.enrolled_at) FROM enrollments e WHERE e.student_id = s.student_id) AS first_enrollment_enrolled_at_2,
        (SELECT COUNT(*) FILTER (WHERE e.term IN ('2024T1', '2024T2', '2025T1')) FROM enrollments e WHERE e.student_id = s.student_id) AS enrollment_term_count
    FROM with_stats s
),
student_id_stats AS (
    -- Avg prior_score_7d per student_id
    SELECT student_id, AVG(prior_score_7d) AS avg_prior_score_7d
    FROM summarized_rows
    GROUP BY student_id
),
vs_group AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_prior_score_7d,
        ROUND(s.prior_score_7d - g.avg_prior_score_7d, 2) AS prior_score_7d_minus_avg
    FROM summarized_rows s
    JOIN student_id_stats g ON g.student_id = s.student_id
    WHERE s.prior_score_7d > g.avg_prior_score_7d
)
SELECT
    id,
    student_id,
    enrolled_at,
    prior_rows_7d,
    prior_score_7d,
    first_enrollment_enrolled_at,
    last_enrollment_enrolled_at,
    first_enrollment_enrolled_at_2,
    enrollment_term_count,
    avg_prior_score_7d,
    prior_score_7d_minus_avg,
    enrolled_at + INTERVAL 30 MINUTES AS enrolled_at_plus_30m
FROM vs_group
ORDER BY student_id DESC NULLS FIRST, enrolled_at DESC, id
LIMIT 10
