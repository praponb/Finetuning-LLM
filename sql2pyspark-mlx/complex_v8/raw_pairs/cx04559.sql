WITH valid AS (
    -- Scope the courses rows
    SELECT
        id,
        course_code,
        dept,
        credits
    FROM courses
    WHERE level = 'advanced'
),
line_level AS (
    -- One row per enrollment
    SELECT
        s.id,
        s.course_code,
        e.id AS enrollment_id,
        e.student_id AS enrollment_student_id,
        e.enrolled_at AS enrollment_enrolled_at,
        e.score AS enrollment_score,
        e.grade_status AS enrollment_grade_status
    FROM valid s
    JOIN enrollments e ON e.course_id = s.id
),
per_group AS (
    -- Summarise per group
    SELECT
        enrollment_student_id,
        DATE_TRUNC('MONTH', enrollment_enrolled_at) AS enrollment_enrolled_at_month,
        ROUND(AVG(enrollment_score), 2) AS avg_enrollment_score,
        ROUND(100 * COUNT(CASE WHEN LENGTH(TRIM(course_code)) >= 8 THEN 1 END) / COUNT(*), 1) AS pct_length_trim_course_code_8,
        SUM(enrollment_score) AS total_enrollment_score
    FROM line_level
    GROUP BY enrollment_student_id, DATE_TRUNC('MONTH', enrollment_enrolled_at)
),
exploded_detail AS (
    -- One row per enrollment
    SELECT
        s.enrollment_student_id,
        s.enrollment_enrolled_at_month,
        s.total_enrollment_score,
        e.id AS enrollment_id,
        e.term AS enrollment_term,
        e.course_id AS enrollment_course_id,
        e.enrolled_at AS enrollment_enrolled_at
    FROM per_group s
    JOIN enrollments e ON e.student_id = s.enrollment_student_id
),
combined_details AS (
    -- Enrich with dimension data
    SELECT
        s.enrollment_student_id,
        s.enrollment_enrolled_at_month,
        s.total_enrollment_score,
        s.enrollment_id,
        s.enrollment_term,
        s.enrollment_course_id,
        s.enrollment_enrolled_at,
        st.enrolled_on AS student_enrolled_on,
        st.cohort AS student_cohort,
        st.gpa AS student_gpa
    FROM exploded_detail s
    JOIN students st ON st.id = s.enrollment_student_id
)
-- Subtotals with CUBE
SELECT
    enrollment_course_id,
    enrollment_student_id,
    COALESCE(student_cohort, 'ALL') AS student_cohort,
    GROUPING(enrollment_course_id) AS is_total_enrollment_course_id,
    COUNT(*) FILTER (WHERE enrollment_term IS NOT NULL) AS n_enrollment_term_is_not_null,
    ROUND(SUM(student_gpa), 2) AS total_student_gpa,
    MIN(enrollment_enrolled_at_month) AS first_enrollment_enrolled_at_month
FROM combined_details
GROUP BY CUBE(enrollment_course_id, enrollment_student_id, student_cohort)
ORDER BY enrollment_course_id NULLS LAST, enrollment_student_id NULLS LAST, student_cohort NULLS LAST
