WITH matched AS (
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
with_parent_student AS (
    -- Enrich with dimension data
    SELECT
        s.*,
        st.major AS student_major,
        st.cohort AS student_cohort,
        st.gpa AS student_gpa
    FROM matched s
    JOIN students st ON st.id = s.student_id
    WHERE st.cohort <> 'C2021'
),
without_matches AS (
    -- Compare against the overall max
    SELECT *
    FROM with_parent_student
    WHERE prior_score_3d < (SELECT MAX(prior_score_3d) FROM with_parent_student)
),
with_details AS (
    -- Enrich with dimension data
    SELECT
        s.*,
        st.is_international AS student_is_international,
        st.student_name,
        st.enrolled_on AS student_enrolled_on
    FROM without_matches s
    LEFT JOIN students st ON st.id = s.student_id AND LOWER(st.student_name) LIKE '% f%'
)
SELECT
    id,
    enrolled_at,
    prior_rows_3d,
    student_major,
    student_cohort,
    student_is_international,
    student_name,
    student_enrolled_on,
    GREATEST(prior_rows_3d, prior_score_3d) AS greatest_prior_rows_3d_prior_score_3d,
    ROUND(student_gpa / NULLIF(prior_score_3d, 0), 3) AS student_gpa_per_prior_score_3d
FROM with_details
ORDER BY id, enrolled_at
