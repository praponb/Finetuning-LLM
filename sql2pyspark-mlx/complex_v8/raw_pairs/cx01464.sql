WITH prepared_enrollments AS (
    -- Filter enrollments
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        CASE WHEN score >= 74.9 THEN 'A' WHEN score >= 20.6 THEN 'B' ELSE 'C' END AS score_tier,
        CONCAT(grade_status, '#', CAST(id AS STRING)) AS grade_status_ref,
        SUBSTRING(TRIM(term), 1, 2) AS term_prefix
    FROM enrollments
    WHERE course_id <= 3
),
with_details_course AS (
    -- Attach course attributes
    SELECT
        s.*,
        c.course_code,
        c.level AS course_level,
        c.credits AS course_credits
    FROM prepared_enrollments s
    JOIN courses c ON c.id = s.course_id
),
line_level AS (
    -- One row per enrollment
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.grade_status,
        s.enrolled_at,
        s.grade_status_ref,
        s.course_credits,
        e.id AS enrollment_id,
        e.enrolled_at AS enrollment_enrolled_at,
        e.term AS enrollment_term,
        e.student_id AS enrollment_student_id
    FROM with_details_course s
    LEFT JOIN enrollments e ON e.course_id = s.course_id
)
SELECT
    id,
    student_id,
    course_id,
    grade_status,
    grade_status_ref,
    enrollment_id,
    enrollment_enrolled_at,
    enrollment_term,
    COALESCE(course_credits, 0) AS course_credits_filled
FROM line_level
ORDER BY course_id DESC
