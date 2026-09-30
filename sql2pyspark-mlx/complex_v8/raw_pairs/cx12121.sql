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
scored AS (
    -- Add calculated columns
    SELECT
        *,
        prior_rows_3d - prior_score_3d AS prior_rows_3d_minus_prior_score_3d,
        GREATEST(prior_score_3d, prior_rows_3d) AS greatest_prior_score_3d_prior_rows_3d
    FROM covered
    WHERE prior_rows_3d > 2
),
enriched_student AS (
    -- Join students
    SELECT
        s.id,
        s.student_id,
        s.enrolled_at,
        s.prior_rows_3d,
        s.prior_rows_3d_minus_prior_score_3d,
        s.greatest_prior_score_3d_prior_rows_3d,
        st.major AS student_major,
        st.student_name,
        st.is_international AS student_is_international
    FROM scored s
    LEFT JOIN students st ON st.id = s.student_id
),
enriched_student_2 AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.student_id,
        s.enrolled_at,
        s.prior_rows_3d,
        s.prior_rows_3d_minus_prior_score_3d,
        s.greatest_prior_score_3d_prior_rows_3d,
        s.student_major,
        s.student_name,
        s.student_is_international,
        st.student_name AS student_student_name,
        st.enrolled_on AS student_enrolled_on
    FROM enriched_student s
    JOIN students st ON st.id = s.student_id
)
-- Final projection
SELECT
    id,
    student_id,
    enrolled_at,
    prior_rows_3d,
    prior_rows_3d_minus_prior_score_3d,
    greatest_prior_score_3d_prior_rows_3d,
    student_is_international,
    student_student_name,
    student_enrolled_on,
    LENGTH(TRIM(student_major)) AS student_major_len
FROM enriched_student_2
ORDER BY student_enrolled_on, student_student_name
