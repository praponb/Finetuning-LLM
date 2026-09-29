WITH scoped AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        CONCAT_WS('|', grade_status, term) AS grade_status_term_label,
        DATEDIFF(DATE '2026-01-01', enrolled_at) AS days_since_enrolled_at
    FROM enrollments
    WHERE HOUR(enrolled_at) BETWEEN 8 AND 13
),
with_children AS (
    -- One row per enrollment
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.term,
        s.score,
        s.grade_status,
        s.days_since_enrolled_at,
        e.id AS enrollment_id,
        e.enrolled_at AS enrollment_enrolled_at,
        e.grade_status AS enrollment_grade_status,
        e.score AS enrollment_score,
        e.student_id AS enrollment_student_id
    FROM scoped s
    JOIN enrollments e ON e.course_id = s.course_id AND e.enrolled_at >= TIMESTAMP '2025-01-13 11:00:00'
),
enriched AS (
    -- Label and score rows
    SELECT
        id,
        term,
        score,
        grade_status,
        days_since_enrolled_at,
        enrollment_id,
        enrollment_score,
        DATE_TRUNC('WEEK', enrollment_enrolled_at) AS enrollment_enrolled_at_week,
        ROUND(enrollment_score * 1.5, 2) AS enrollment_score_adj,
        FLOOR(days_since_enrolled_at / 100) * 100 AS days_since_enrolled_at_bucket
    FROM with_children
    WHERE enrollment_grade_status IS NOT NULL
),
enriched_enrollment AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.term,
        s.grade_status,
        s.days_since_enrolled_at,
        s.enrollment_id,
        s.enrollment_score,
        s.enrollment_score_adj,
        s.days_since_enrolled_at_bucket,
        e.student_id AS enrollment_student_id,
        e.enrolled_at AS enrollment_enrolled_at,
        c.dept AS course_dept,
        c.course_code
    FROM enriched s
    JOIN enrollments e ON e.id = s.enrollment_id
    LEFT JOIN courses c ON c.id = e.course_id
),
windowed AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        term,
        grade_status,
        days_since_enrolled_at,
        enrollment_id,
        enrollment_score,
        enrollment_score_adj,
        days_since_enrolled_at_bucket,
        enrollment_student_id,
        enrollment_enrolled_at,
        course_dept,
        course_code,
        LAG(days_since_enrolled_at) OVER (PARTITION BY enrollment_id, course_dept ORDER BY days_since_enrolled_at, id, enrollment_id) AS prev_days_since_enrolled_at,
        LAST_VALUE(enrollment_enrolled_at) OVER (PARTITION BY enrollment_id, course_dept ORDER BY days_since_enrolled_at, id, enrollment_id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_enrollment_enrolled_at
    FROM enriched_enrollment
),
joined_enrollment AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.grade_status,
        s.enrollment_id,
        s.enrollment_score,
        s.days_since_enrolled_at_bucket,
        s.enrollment_student_id,
        s.enrollment_enrolled_at,
        s.course_dept,
        s.course_code,
        s.prev_days_since_enrolled_at,
        s.last_enrollment_enrolled_at,
        e.course_id AS enrollment_course_id,
        e.term AS enrollment_term,
        e.student_id AS enrollment_student_id_2,
        st.is_international AS student_is_international,
        st.gpa AS student_gpa
    FROM windowed s
    JOIN enrollments e ON e.id = s.enrollment_id
    JOIN students st ON st.id = e.student_id
)
SELECT
    id,
    grade_status,
    enrollment_id,
    enrollment_score,
    days_since_enrolled_at_bucket,
    enrollment_student_id,
    enrollment_enrolled_at,
    course_code,
    prev_days_since_enrolled_at,
    last_enrollment_enrolled_at,
    enrollment_term,
    student_is_international,
    student_gpa,
    LOWER(REGEXP_REPLACE(TRIM(grade_status), ' ', '_')) AS grade_status_slug,
    LOWER(REGEXP_REPLACE(TRIM(course_code), ' ', '_')) AS course_code_slug
FROM joined_enrollment
ORDER BY enrollment_enrolled_at DESC, student_gpa DESC
