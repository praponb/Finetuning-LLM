WITH in_window AS (
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
combined_details_student AS (
    -- Join students
    SELECT
        s.id,
        s.student_id,
        s.prior_rows_1d,
        st.major AS student_major
    FROM in_window s
    JOIN students st ON st.id = s.student_id
),
with_windows AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        student_id,
        prior_rows_1d,
        student_major,
        SUM(prior_rows_1d) OVER (ORDER BY prior_rows_1d DESC, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS prior_rows_1d_sum_4,
        ROUND(CUME_DIST() OVER (ORDER BY prior_rows_1d DESC, id), 3) AS prior_rows_1d_cume_dist,
        ROUND(prior_rows_1d - AVG(prior_rows_1d) OVER w_student_id_student_major, 2) AS prior_rows_1d_vs_student_id_student_major_avg,
        ROUND(CUME_DIST() OVER w_student_major_by_prior_rows, 3) AS prior_rows_1d_cume_dist_2
    FROM combined_details_student
    WINDOW w_student_id_student_major AS (PARTITION BY student_id, student_major), w_student_major_by_prior_rows AS (PARTITION BY student_major ORDER BY prior_rows_1d DESC)
),
with_counts AS (
    -- Attach enrollments metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FILTER (WHERE e.term IN ('2025T2', '2024T2')) FROM enrollments e WHERE e.student_id = s.student_id) AS enrollment_term_count,
        (SELECT MIN(e.enrolled_at) FROM enrollments e WHERE e.student_id = s.student_id) AS first_enrollment_enrolled_at
    FROM with_windows s
)
SELECT id, field, metric_value
FROM (
    SELECT id, prior_rows_1d_sum_4, prior_rows_1d
    FROM with_counts
) src
UNPIVOT (
    metric_value FOR field IN (prior_rows_1d_sum_4, prior_rows_1d)
) unp
ORDER BY id, field
