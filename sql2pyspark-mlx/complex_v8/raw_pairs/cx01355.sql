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
enriched AS (
    -- Attach student attributes
    SELECT
        s.id,
        s.student_id,
        s.prior_score_7d,
        st.is_international AS student_is_international,
        st.gpa AS student_gpa
    FROM covered s
    JOIN students st ON st.id = s.student_id
    WHERE st.gpa < 3.3
),
with_windows AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROUND(CUME_DIST() OVER (PARTITION BY student_is_international ORDER BY student_gpa DESC, id), 3) AS student_gpa_cume_dist,
        RANK() OVER (PARTITION BY student_is_international ORDER BY student_gpa DESC) AS student_gpa_rank
    FROM enriched
),
sequenced AS (
    -- Previous / next values per key
    SELECT
        *,
        ROUND(prior_score_7d - AVG(prior_score_7d) OVER (PARTITION BY student_id, student_is_international), 2) AS prior_score_7d_vs_student_id_student_is_internat,
        NTILE(10) OVER (PARTITION BY student_is_international, student_gpa_rank ORDER BY student_gpa_cume_dist, id) AS student_gpa_cume_dist_decile
    FROM with_windows
)
-- Subtotals with CUBE
SELECT
    student_id,
    student_is_international,
    GROUPING(student_id) AS is_total_student_id,
    MIN(prior_score_7d) AS min_prior_score_7d,
    SUM(student_gpa_cume_dist_decile) FILTER (WHERE student_is_international = FALSE) AS student_gpa_cume_dist_decile_student_is_internat,
    ROUND(AVG(student_gpa_cume_dist), 2) AS avg_student_gpa_cume_dist
FROM sequenced
GROUP BY CUBE(student_id, student_is_international)
ORDER BY student_id NULLS LAST, student_is_international NULLS LAST
