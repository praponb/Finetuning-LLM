WITH joined_ranges AS (
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
combined_details AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.student_id,
        s.prior_rows_3d,
        s.prior_score_3d,
        st.enrolled_on AS student_enrolled_on
    FROM joined_ranges s
    LEFT JOIN students st ON st.id = s.student_id AND LOWER(st.student_name) LIKE '%ha%'
),
with_parent AS (
    -- Attach student attributes
    SELECT s.*, st.cohort AS student_cohort, st.student_name
    FROM combined_details s
    JOIN students st ON st.id = s.student_id
    WHERE st.cohort = 'C2023'
),
windowed AS (
    -- Previous / next values per key
    SELECT
        *,
        ROUND(prior_rows_3d - AVG(prior_rows_3d) OVER (PARTITION BY student_id), 2) AS prior_rows_3d_vs_student_id_avg,
        NTH_VALUE(student_enrolled_on, 2) OVER (PARTITION BY student_id, student_cohort ORDER BY prior_rows_3d DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_student_enrolled_on,
        LAST_VALUE(prior_rows_3d) IGNORE NULLS OVER (PARTITION BY student_id, student_cohort ORDER BY prior_rows_3d DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_prior_rows_3d
    FROM with_parent
)
-- Final projection
SELECT
    id,
    student_id,
    prior_rows_3d,
    student_cohort,
    student_name,
    prior_rows_3d_vs_student_id_avg,
    last_known_prior_rows_3d
FROM windowed
