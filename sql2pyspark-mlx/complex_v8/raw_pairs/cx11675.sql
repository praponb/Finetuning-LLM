WITH cleaned_enrollments AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        COALESCE(score, 0.0) AS score_filled
    FROM enrollments
),
exploded_detail_enrollments AS (
    -- One row per enrollment
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.term,
        s.enrolled_at,
        s.score_filled,
        e.id AS enrollment_id,
        e.term AS enrollment_term,
        e.enrolled_at AS enrollment_enrolled_at
    FROM cleaned_enrollments s
    JOIN enrollments e ON e.student_id = s.student_id
),
with_children_enrollments AS (
    -- One row per enrollment
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.term,
        s.enrolled_at,
        s.score_filled,
        s.enrollment_id,
        s.enrollment_term,
        s.enrollment_enrolled_at,
        e.id AS enrollment_id_2,
        e.course_id AS enrollment_course_id,
        e.enrolled_at AS enrollment_enrolled_at_2
    FROM exploded_detail_enrollments s
    LEFT JOIN enrollments e ON e.student_id = s.student_id
)
SELECT
    id,
    course_id,
    term,
    enrolled_at,
    score_filled,
    enrollment_id,
    enrollment_term,
    enrollment_enrolled_at,
    enrollment_id_2,
    enrollment_enrolled_at_2,
    CONCAT_WS('|', term, enrollment_term) AS term_enrollment_term_label,
    CASE WHEN score_filled >= 60.0 THEN 'large' WHEN score_filled >= 20.0 THEN 'medium' ELSE 'small' END AS score_filled_tier
FROM with_children_enrollments
ORDER BY id DESC NULLS FIRST, term DESC, enrollment_id, enrollment_id_2
LIMIT 10
