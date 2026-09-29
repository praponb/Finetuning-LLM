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
        LENGTH(TRIM(grade_status)) AS grade_status_len,
        CONCAT_WS('|', grade_status, term) AS grade_status_term_label,
        UPPER(TRIM(grade_status)) AS grade_status_clean
    FROM enrollments
    WHERE score >= 32.7
      AND TO_DATE(enrolled_at) < DATE '2025-02-06'
),
exploded_detail AS (
    -- One row per enrollment
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.grade_status,
        s.enrolled_at,
        s.grade_status_len,
        e.id AS enrollment_id,
        e.grade_status AS enrollment_grade_status,
        e.term AS enrollment_term,
        e.course_id AS enrollment_course_id,
        e.enrolled_at AS enrollment_enrolled_at
    FROM base s
    JOIN enrollments e ON e.student_id = s.student_id AND e.course_id <= 5
)
SELECT
    enrollment_id,
    student_id,
    grade_status_len,
    MIN(enrollment_enrolled_at) AS first_enrollment_enrolled_at,
    COUNT(*) AS cnt,
    SUM(grade_status_len) AS total_grade_status_len
FROM exploded_detail
GROUP BY CUBE(enrollment_id, student_id, grade_status_len)
