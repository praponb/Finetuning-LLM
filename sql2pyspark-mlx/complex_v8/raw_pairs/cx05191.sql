WITH filtered AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        CONCAT_WS(' / ', term, grade_status) AS term_grade_status_label,
        SUBSTRING(TRIM(term), 1, 4) AS term_prefix
    FROM enrollments
    WHERE grade_status NOT IN ('FAIL', 'PASS')
      AND score > 8.7
),
derived AS (
    -- Derive helper columns
    SELECT
        id,
        student_id,
        term,
        score,
        term_grade_status_label,
        term_prefix,
        ABS(score - 63.9) AS score_distance,
        ROUND(score * 1.07, 2) AS score_adj,
        DATEDIFF(DATE '2025-12-31', enrolled_at) AS days_since_enrolled_at
    FROM filtered
    WHERE score >= 36.6
),
joined AS (
    -- Enrich with dimension data
    SELECT
        s.*,
        st.enrolled_on AS student_enrolled_on,
        st.cohort AS student_cohort,
        st.is_international AS student_is_international
    FROM derived s
    JOIN students st ON st.id = s.student_id
    WHERE st.is_international = FALSE
),
exploded_detail_enrollments AS (
    -- One row per enrollment
    SELECT
        s.id,
        s.student_id,
        s.term,
        s.score_adj,
        s.student_enrolled_on,
        s.student_cohort,
        s.student_is_international,
        e.id AS enrollment_id,
        e.course_id AS enrollment_course_id,
        e.grade_status AS enrollment_grade_status,
        e.enrolled_at AS enrollment_enrolled_at,
        e.term AS enrollment_term
    FROM joined s
    JOIN enrollments e ON e.student_id = s.student_id
)
-- Final projection
SELECT
    id,
    student_id,
    student_cohort,
    student_is_international,
    enrollment_id,
    enrollment_grade_status,
    enrollment_term
FROM exploded_detail_enrollments
WHERE student_is_international = TRUE
ORDER BY enrollment_grade_status, enrollment_id
