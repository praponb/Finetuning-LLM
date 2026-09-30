WITH filtered AS (
    -- Scope the enrollments rows
    SELECT
        id,
        student_id,
        course_id,
        term,
        grade_status,
        enrolled_at,
        LOWER(REGEXP_REPLACE(TRIM(term), ' ', '_')) AS term_slug,
        CONCAT_WS('-', term, grade_status) AS term_grade_status_label
    FROM enrollments
    WHERE course_id IS NOT NULL
      AND term IN ('2025T1', '2024T1')
),
enriched AS (
    -- Attach student attributes
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.term,
        s.grade_status,
        s.enrolled_at,
        s.term_slug,
        s.term_grade_status_label,
        st.gpa AS student_gpa,
        st.major AS student_major
    FROM filtered s
    JOIN students st ON st.id = s.student_id
),
with_trends AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        MAX(student_gpa) OVER (PARTITION BY YEAR(enrolled_at), course_id ORDER BY student_gpa, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS student_gpa_max_4,
        FIRST_VALUE(enrolled_at) OVER (PARTITION BY course_id, grade_status ORDER BY enrolled_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_enrolled_at,
        NTH_VALUE(student_gpa, 2) OVER (PARTITION BY course_id, grade_status ORDER BY enrolled_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_student_gpa,
        ROUND(CUME_DIST() OVER (ORDER BY student_gpa), 3) AS student_gpa_cume_dist,
        DENSE_RANK() OVER (PARTITION BY course_id ORDER BY student_gpa DESC NULLS FIRST) AS student_gpa_dense_rank,
        ROUND(AVG(student_gpa) OVER (PARTITION BY term), 2) AS term_avg_student_gpa,
        ROUND(student_gpa - AVG(student_gpa) OVER (PARTITION BY course_id, term), 2) AS student_gpa_vs_course_id_term_avg,
        ROUND(student_gpa - AVG(student_gpa) OVER (PARTITION BY term), 2) AS student_gpa_vs_term_avg,
        ROUND(student_gpa - AVG(student_gpa) OVER (PARTITION BY course_id), 2) AS student_gpa_vs_course_id_avg
    FROM enriched
),
ranked AS (
    -- Running and moving aggregates
    SELECT
        id,
        student_id,
        course_id,
        grade_status,
        enrolled_at,
        student_gpa,
        student_major,
        first_enrolled_at,
        second_student_gpa,
        student_gpa_dense_rank,
        student_gpa_vs_term_avg,
        student_gpa_vs_course_id_avg,
        ROUND(CUME_DIST() OVER (PARTITION BY student_gpa_dense_rank, student_id ORDER BY student_gpa_max_4 DESC, id), 3) AS student_gpa_max_4_cume_dist,
        LEAD(student_gpa_vs_term_avg) OVER (PARTITION BY student_gpa_dense_rank, student_id ORDER BY student_gpa_max_4 DESC, id) AS next_student_gpa_vs_term_avg,
        ROUND(AVG(second_student_gpa) OVER w_course_id, 2) AS course_id_avg_second_student_gpa,
        MAX(student_gpa_vs_course_id_avg) OVER (PARTITION BY student_gpa_dense_rank, student_id ORDER BY student_gpa_max_4 DESC, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS student_gpa_vs_course_id_avg_max_2,
        DENSE_RANK() OVER (PARTITION BY grade_status, student_major ORDER BY term_avg_student_gpa) AS term_avg_student_gpa_dense_rank
    FROM with_trends
    WINDOW w_course_id AS (PARTITION BY course_id)
)
-- Final projection
SELECT
    id,
    student_id,
    grade_status,
    enrolled_at,
    student_gpa,
    student_major,
    first_enrolled_at,
    second_student_gpa,
    student_gpa_vs_term_avg,
    student_gpa_vs_course_id_avg,
    student_gpa_max_4_cume_dist,
    next_student_gpa_vs_term_avg,
    term_avg_student_gpa_dense_rank,
    SUBSTRING(TRIM(student_major), 1, 3) AS student_major_prefix
FROM ranked
WHERE student_gpa_max_4_cume_dist < 0.6
ORDER BY next_student_gpa_vs_term_avg, id
LIMIT 20
