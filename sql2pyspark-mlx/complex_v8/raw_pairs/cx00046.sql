WITH base AS (
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
        DATEDIFF(DATE '2026-01-01', enrolled_at) AS days_since_enrolled_at
    FROM enrollments
    WHERE student_id <= 6
),
enriched AS (
    -- Label and score rows
    SELECT
        id,
        student_id,
        days_since_enrolled_at,
        COALESCE(grade_status, 'UNKNOWN') AS grade_status_or_unknown
    FROM base
    WHERE course_id IS NOT NULL
),
with_details AS (
    -- Join students
    SELECT s.*, st.gpa AS student_gpa, st.enrolled_on AS student_enrolled_on
    FROM enriched s
    LEFT JOIN students st ON st.id = s.student_id
),
line_level_enrollments AS (
    -- One row per enrollment
    SELECT
        s.id,
        s.student_id,
        s.days_since_enrolled_at,
        s.grade_status_or_unknown,
        e.id AS enrollment_id,
        e.term AS enrollment_term,
        e.grade_status AS enrollment_grade_status
    FROM with_details s
    JOIN enrollments e ON e.student_id = s.student_id
),
metrics AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        RANK() OVER (ORDER BY days_since_enrolled_at DESC) AS days_since_enrolled_at_rank,
        SUM(days_since_enrolled_at) OVER w_student_id_enrollment_grade AS student_id_enrollment_grade_status_sum_days_sinc
    FROM line_level_enrollments
    WINDOW w_student_id_enrollment_grade AS (PARTITION BY student_id, enrollment_grade_status)
),
enriched_student AS (
    -- Join students
    SELECT
        s.id,
        s.student_id,
        s.days_since_enrolled_at,
        s.enrollment_id,
        s.enrollment_term,
        s.days_since_enrolled_at_rank,
        s.student_id_enrollment_grade_status_sum_days_sinc,
        st.student_name,
        st.enrolled_on AS student_enrolled_on,
        st.gpa AS student_gpa,
        e.score AS enrollment_score,
        e.grade_status AS enrollment_grade_status_2
    FROM metrics s
    LEFT JOIN students st ON st.id = s.student_id
    LEFT JOIN enrollments e ON e.id = s.enrollment_id
)
-- Final projection
SELECT
    id,
    student_id,
    enrollment_id,
    enrollment_term,
    days_since_enrolled_at_rank,
    student_id_enrollment_grade_status_sum_days_sinc,
    student_name,
    student_enrolled_on,
    student_gpa,
    enrollment_grade_status_2,
    days_since_enrolled_at - student_gpa AS days_since_enrolled_at_minus_student_gpa,
    COALESCE(enrollment_term, 'UNKNOWN') AS enrollment_term_or_unknown
FROM enriched_student
ORDER BY student_name NULLS LAST, student_enrolled_on NULLS LAST, id, enrollment_id
LIMIT 5
