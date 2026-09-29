WITH joined_ranges AS (
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
joined_student AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.student_id,
        s.enrolled_at,
        s.prior_rows_7d,
        s.prior_score_7d,
        st.enrolled_on AS student_enrolled_on
    FROM joined_ranges s
    LEFT JOIN students st ON st.id = s.student_id
),
with_parent AS (
    -- Attach student attributes
    SELECT
        s.id,
        s.student_id,
        s.enrolled_at,
        s.prior_rows_7d,
        s.prior_score_7d,
        s.student_enrolled_on,
        st.cohort AS student_cohort
    FROM joined_student s
    JOIN students st ON st.id = s.student_id AND st.enrolled_on >= DATE '2024-03-15'
),
student_id_stats AS (
    -- Median prior_rows_7d per student_id
    SELECT student_id, PERCENTILE_APPROX(prior_rows_7d, 0.5) AS median_prior_rows_7d
    FROM with_parent
    GROUP BY student_id
),
outliers AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.median_prior_rows_7d,
        ROUND(s.prior_rows_7d - g.median_prior_rows_7d, 2) AS prior_rows_7d_minus_median
    FROM with_parent s
    JOIN student_id_stats g ON g.student_id = s.student_id
    WHERE s.prior_rows_7d >= g.median_prior_rows_7d
)
SELECT
    id,
    prior_rows_7d,
    prior_score_7d,
    student_enrolled_on,
    student_cohort,
    median_prior_rows_7d,
    prior_rows_7d_minus_median,
    enrolled_at + INTERVAL 2 HOURS AS enrolled_at_plus_2h
FROM outliers
