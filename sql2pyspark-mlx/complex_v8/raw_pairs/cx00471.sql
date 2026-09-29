WITH cleaned AS (
    -- Scope the enrollments rows
    SELECT
        id,
        student_id,
        course_id,
        term,
        grade_status,
        enrolled_at,
        UPPER(TRIM(grade_status)) AS grade_status_clean
    FROM enrollments
    WHERE course_id IS NOT NULL
      AND grade_status = 'PASS'
),
with_parent_student AS (
    -- Enrich with dimension data
    SELECT s.*, st.gpa AS student_gpa
    FROM cleaned s
    JOIN students st ON st.id = s.student_id
    WHERE st.gpa <= 3.4
),
windowed AS (
    -- Running and moving aggregates
    SELECT
        *,
        FIRST_VALUE(enrolled_at) OVER (ORDER BY student_gpa DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_enrolled_at,
        NTH_VALUE(student_gpa, 2) OVER (ORDER BY student_gpa DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_student_gpa,
        FIRST_VALUE(enrolled_at) IGNORE NULLS OVER (PARTITION BY term ORDER BY student_gpa, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_enrolled_at,
        SUM(student_gpa) OVER (PARTITION BY grade_status) AS grade_status_sum_student_gpa,
        ROUND(CUME_DIST() OVER w_all_by_student_gpa_ties, 3) AS student_gpa_cume_dist,
        student_gpa - LAG(student_gpa) OVER (ORDER BY student_gpa DESC NULLS FIRST, id) AS student_gpa_change,
        COUNT(*) OVER w_grade_status AS rows_in_grade_status,
        ROUND(student_gpa / NULLIF(SUM(student_gpa) OVER (PARTITION BY grade_status), 0), 4) AS student_gpa_share_of_grade_status,
        DENSE_RANK() OVER (PARTITION BY term ORDER BY student_gpa, id) AS student_gpa_dense_rank
    FROM with_parent_student
    WINDOW w_all_by_student_gpa_ties AS (ORDER BY student_gpa DESC NULLS FIRST), w_grade_status AS (PARTITION BY grade_status)
)
-- Final projection
SELECT
    id,
    course_id,
    term,
    grade_status,
    enrolled_at,
    grade_status_clean,
    student_gpa,
    first_enrolled_at,
    first_known_enrolled_at,
    grade_status_sum_student_gpa,
    student_gpa_cume_dist,
    student_gpa_change,
    rows_in_grade_status,
    student_gpa_share_of_grade_status,
    student_gpa_dense_rank,
    LOWER(REGEXP_REPLACE(TRIM(term), ' ', '_')) AS term_slug
FROM windowed
