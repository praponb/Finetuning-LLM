WITH valid AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        enrolled_at + INTERVAL 15 MINUTES AS enrolled_at_plus_15m,
        CONCAT(term, '#', CAST(id AS STRING)) AS term_ref
    FROM enrollments
),
enriched_course AS (
    -- Join courses
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.score,
        s.grade_status,
        s.enrolled_at,
        s.enrolled_at_plus_15m,
        s.term_ref,
        c.level AS course_level,
        c.dept AS course_dept,
        c.course_code
    FROM valid s
    LEFT JOIN courses c ON c.id = s.course_id
),
enriched AS (
    -- Add calculated columns
    SELECT *, ABS(score - 35.8) AS score_distance
    FROM enriched_course
    WHERE course_dept IS NOT NULL
),
detail_rows_enrollments AS (
    -- One row per enrollment
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.grade_status,
        s.enrolled_at,
        s.enrolled_at_plus_15m,
        s.term_ref,
        s.course_level,
        s.course_dept,
        s.course_code,
        s.score_distance,
        e.id AS enrollment_id,
        e.term AS enrollment_term,
        e.student_id AS enrollment_student_id,
        e.score AS enrollment_score
    FROM enriched s
    JOIN enrollments e ON e.course_id = s.course_id AND e.student_id >= 7
),
stats AS (
    -- Group-level totals
    SELECT
        grade_status,
        enrollment_student_id,
        YEAR(enrolled_at_plus_15m) AS enrolled_at_plus_15m_year,
        COUNT(*) AS row_count,
        ROUND(SUM(enrollment_score), 2) AS total_enrollment_score,
        SUM(enrollment_score) AS total_enrollment_score_2
    FROM detail_rows_enrollments
    GROUP BY grade_status, enrollment_student_id, YEAR(enrolled_at_plus_15m)
),
joined_student AS (
    -- Enrich with dimension data
    SELECT
        s.grade_status,
        s.enrollment_student_id,
        s.enrolled_at_plus_15m_year,
        s.row_count,
        s.total_enrollment_score_2,
        st.gpa AS student_gpa,
        st.enrolled_on AS student_enrolled_on,
        st.cohort AS student_cohort
    FROM stats s
    LEFT JOIN students st ON st.id = s.enrollment_student_id
)
SELECT
    grade_status,
    enrollment_student_id,
    enrolled_at_plus_15m_year,
    row_count,
    total_enrollment_score_2,
    student_gpa,
    student_cohort,
    student_gpa + total_enrollment_score_2 AS student_gpa_plus_total_enrollment_score_2
FROM joined_student
ORDER BY student_gpa
