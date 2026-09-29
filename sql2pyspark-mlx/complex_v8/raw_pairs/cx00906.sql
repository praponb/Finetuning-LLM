WITH scoped_enrollments AS (
    -- Filter enrollments
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        DATEDIFF(DATE '2026-01-01', enrolled_at) AS days_since_enrolled_at,
        LENGTH(TRIM(grade_status)) AS grade_status_len,
        LENGTH(TRIM(term)) AS term_len
    FROM enrollments
    WHERE course_id >= 6
),
line_level_enrollments AS (
    -- One row per enrollment
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.score,
        s.days_since_enrolled_at,
        s.grade_status_len,
        s.term_len,
        e.id AS enrollment_id,
        e.enrolled_at AS enrollment_enrolled_at,
        e.term AS enrollment_term
    FROM scoped_enrollments s
    JOIN enrollments e ON e.student_id = s.student_id AND e.term = '2025T1'
),
with_details_enrollment AS (
    -- Attach enrollment attributes
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.enrollment_id,
        s.enrollment_enrolled_at,
        e.student_id AS enrollment_student_id,
        e.enrolled_at AS enrollment_enrolled_at_2,
        st.is_international AS student_is_international,
        st.enrolled_on AS student_enrolled_on
    FROM line_level_enrollments s
    JOIN enrollments e ON e.id = s.enrollment_id
    LEFT JOIN students st ON st.id = s.student_id
)
-- Subtotals with ROLLUP
SELECT
    enrollment_id,
    enrollment_student_id,
    course_id,
    COUNT(*) FILTER (WHERE NOT student_is_international) AS n_not_student_is_international
FROM with_details_enrollment
GROUP BY ROLLUP(enrollment_id, enrollment_student_id, course_id)
