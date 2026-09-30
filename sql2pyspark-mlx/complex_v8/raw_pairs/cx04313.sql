WITH recent_enrollments AS (
    -- Filter enrollments
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        FLOOR(score / 25) * 25 AS score_bucket,
        LOWER(REGEXP_REPLACE(TRIM(grade_status), ' ', '_')) AS grade_status_slug,
        LENGTH(TRIM(grade_status)) AS grade_status_len
    FROM enrollments
),
detail_rows AS (
    -- One row per enrollment
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.grade_status,
        s.enrolled_at,
        e.id AS enrollment_id,
        e.term AS enrollment_term,
        e.enrolled_at AS enrollment_enrolled_at,
        e.score AS enrollment_score
    FROM recent_enrollments s
    LEFT JOIN enrollments e ON e.course_id = s.course_id AND e.enrolled_at < TIMESTAMP '2025-02-24 13:00:00'
),
joined AS (
    -- Join enrollments
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.grade_status,
        s.enrolled_at,
        s.enrollment_id,
        s.enrollment_term,
        s.enrollment_enrolled_at,
        e.grade_status AS enrollment_grade_status,
        e.term AS enrollment_term_2,
        e.student_id AS enrollment_student_id,
        st.major AS student_major,
        st.enrolled_on AS student_enrolled_on
    FROM detail_rows s
    JOIN enrollments e ON e.id = s.enrollment_id
    LEFT JOIN students st ON st.id = e.student_id
)
-- Final projection
SELECT
    id,
    student_id,
    course_id,
    enrollment_id,
    enrollment_term,
    enrollment_enrolled_at,
    enrollment_grade_status,
    enrollment_term_2,
    enrollment_student_id,
    student_major,
    CONCAT(enrollment_term_2, '#', CAST(course_id AS STRING)) AS enrollment_term_2_ref,
    CONCAT(enrollment_term_2, '#', CAST(enrollment_student_id AS STRING)) AS enrollment_term_2_ref_2
FROM joined
ORDER BY enrollment_grade_status, id, enrollment_id
LIMIT 20
