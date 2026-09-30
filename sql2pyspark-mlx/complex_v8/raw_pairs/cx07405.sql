WITH filtered_enrollments AS (
    -- Filter enrollments
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        COALESCE(score, 0.0) AS score_filled,
        COALESCE(term, 'UNKNOWN') AS term_or_unknown,
        ROUND(score * 0.25, 2) AS score_adj
    FROM enrollments
    WHERE score < 73.0
),
line_level_enrollments AS (
    -- One row per enrollment
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.score,
        s.enrolled_at,
        e.id AS enrollment_id,
        e.term AS enrollment_term,
        e.grade_status AS enrollment_grade_status,
        e.student_id AS enrollment_student_id,
        e.score AS enrollment_score
    FROM filtered_enrollments s
    JOIN enrollments e ON e.course_id = s.course_id AND e.score <= 79.2
),
combined_details AS (
    -- Attach student attributes
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.score,
        s.enrolled_at,
        s.enrollment_id,
        s.enrollment_term,
        s.enrollment_grade_status,
        s.enrollment_student_id,
        s.enrollment_score,
        st.enrolled_on AS student_enrolled_on,
        st.cohort AS student_cohort,
        st.is_international AS student_is_international,
        c.course_code
    FROM line_level_enrollments s
    LEFT JOIN students st ON st.id = s.student_id
    LEFT JOIN courses c ON c.id = s.course_id
),
stats AS (
    -- Group-level totals
    SELECT
        DATE_FORMAT(student_enrolled_on, 'yyyy-MM') AS student_enrolled_on_ym,
        student_is_international,
        student_id,
        ROUND(SUM(score), 2) AS total_score,
        MIN(enrolled_at) AS first_enrolled_at,
        ROUND(AVG(enrollment_score), 2) AS avg_enrollment_score,
        COUNT(*) FILTER (WHERE student_is_international = FALSE) AS n_student_is_international_fal,
        MAX(score) AS max_score
    FROM combined_details
    WHERE enrollment_score BETWEEN 17.4 AND 94.7
    GROUP BY 1, 2, 3
)
-- Final projection
SELECT
    student_enrolled_on_ym,
    avg_enrollment_score,
    n_student_is_international_fal,
    ABS(total_score - 180.0) AS total_score_distance
FROM stats
ORDER BY student_enrolled_on_ym NULLS LAST, total_score_distance NULLS LAST
