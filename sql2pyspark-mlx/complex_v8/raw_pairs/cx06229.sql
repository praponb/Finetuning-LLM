WITH covered AS (
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
with_activity AS (
    -- Compare against the overall max
    SELECT *
    FROM covered
    WHERE prior_score_7d < (SELECT MAX(prior_score_7d) FROM covered)
),
student_id_stats AS (
    -- Avg prior_score_7d per student_id
    SELECT student_id, AVG(prior_score_7d) AS avg_prior_score_7d
    FROM with_activity
    GROUP BY student_id
),
vs_group AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_prior_score_7d,
        ROUND(s.prior_score_7d - g.avg_prior_score_7d, 2) AS prior_score_7d_minus_avg
    FROM with_activity s
    JOIN student_id_stats g ON g.student_id = s.student_id
    WHERE s.prior_score_7d > g.avg_prior_score_7d
),
with_children AS (
    -- One row per enrollment
    SELECT
        s.id,
        s.student_id,
        s.enrolled_at,
        s.prior_score_7d,
        s.avg_prior_score_7d,
        s.prior_score_7d_minus_avg,
        e.id AS enrollment_id,
        e.term AS enrollment_term,
        e.course_id AS enrollment_course_id,
        e.grade_status AS enrollment_grade_status
    FROM vs_group s
    LEFT JOIN enrollments e ON e.student_id = s.student_id
)
-- Subtotals with GROUPING SETS
SELECT
    enrollment_id,
    enrollment_grade_status,
    enrollment_course_id,
    ROUND(AVG(avg_prior_score_7d), 2) AS avg_avg_prior_score_7d,
    MIN(prior_score_7d_minus_avg) AS min_prior_score_7d_minus_avg,
    PERCENTILE_APPROX(prior_score_7d, 0.5) AS median_prior_score_7d
FROM with_children
GROUP BY GROUPING SETS ((enrollment_id, enrollment_grade_status, enrollment_course_id), (enrollment_id), (enrollment_grade_status))
ORDER BY enrollment_id NULLS LAST, enrollment_grade_status NULLS LAST, enrollment_course_id NULLS LAST
