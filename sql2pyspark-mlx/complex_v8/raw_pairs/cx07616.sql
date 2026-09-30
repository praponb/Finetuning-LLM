WITH prepared_enrollments AS (
    -- Scope the enrollments rows
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        CONCAT_WS('-', term, grade_status) AS term_grade_status_label
    FROM enrollments
    WHERE grade_status <> 'INCOMPLETE'
      AND score < 73.7
),
exploded_detail_enrollments AS (
    -- One row per enrollment
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.term,
        s.score,
        s.grade_status,
        s.enrolled_at,
        s.term_grade_status_label,
        e.id AS enrollment_id,
        e.grade_status AS enrollment_grade_status,
        e.score AS enrollment_score,
        e.term AS enrollment_term
    FROM prepared_enrollments s
    JOIN enrollments e ON e.student_id = s.student_id
),
with_details_course AS (
    -- Attach course attributes
    SELECT s.*, c.credits AS course_credits
    FROM exploded_detail_enrollments s
    JOIN courses c ON c.id = s.course_id
)
SELECT id, enrollment_id, metric, reading
FROM (
    SELECT id, enrollment_id, enrollment_score, score
    FROM with_details_course
) src
UNPIVOT INCLUDE NULLS (
    reading FOR metric IN (enrollment_score AS m_enrollment_score, score AS m_score)
) unp
