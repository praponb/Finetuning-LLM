WITH base AS (
    -- Scope the enrollments rows
    SELECT
        id,
        student_id,
        course_id,
        term,
        grade_status,
        enrolled_at,
        INITCAP(TRIM(grade_status)) AS grade_status_clean,
        CONCAT(grade_status, '#', CAST(id AS STRING)) AS grade_status_ref
    FROM enrollments
    WHERE grade_status = 'FAIL'
      AND score <= 57.3
),
with_children_enrollments AS (
    -- One row per enrollment
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.grade_status,
        s.grade_status_clean,
        s.grade_status_ref,
        e.id AS enrollment_id,
        e.term AS enrollment_term,
        e.score AS enrollment_score,
        e.student_id AS enrollment_student_id,
        e.grade_status AS enrollment_grade_status
    FROM base s
    LEFT JOIN enrollments e ON e.course_id = s.course_id
),
combined_details AS (
    -- Join students
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.grade_status,
        s.grade_status_clean,
        s.grade_status_ref,
        s.enrollment_id,
        s.enrollment_term,
        s.enrollment_score,
        s.enrollment_student_id,
        s.enrollment_grade_status,
        st.cohort AS student_cohort
    FROM with_children_enrollments s
    JOIN students st ON st.id = s.student_id
),
with_trends AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        course_id,
        grade_status,
        grade_status_ref,
        enrollment_id,
        enrollment_score,
        enrollment_student_id,
        enrollment_grade_status,
        COUNT(*) OVER (PARTITION BY enrollment_student_id) AS rows_in_enrollment_student_id,
        LAG(enrollment_score, 2, 0) OVER (PARTITION BY student_id, enrollment_id ORDER BY enrollment_score DESC, id, enrollment_id) AS prev2_enrollment_score,
        ROUND(enrollment_score - AVG(enrollment_score) OVER (PARTITION BY enrollment_student_id), 2) AS enrollment_score_vs_enrollment_student_id_avg
    FROM combined_details
)
SELECT
    id,
    course_id,
    enrollment_id,
    enrollment_grade_status,
    rows_in_enrollment_student_id,
    prev2_enrollment_score,
    enrollment_score_vs_enrollment_student_id_avg,
    ROUND(enrollment_score / NULLIF(prev2_enrollment_score, 0), 3) AS enrollment_score_per_prev2_enrollment_score
FROM with_trends
ORDER BY prev2_enrollment_score, id, enrollment_id
LIMIT 5
