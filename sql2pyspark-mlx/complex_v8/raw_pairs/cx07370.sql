WITH filtered_enrollments AS (
    -- Filter enrollments
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        COALESCE(grade_status, 'UNKNOWN') AS grade_status_or_unknown
    FROM enrollments
    WHERE score > 36.6
      AND student_id IS NOT NULL
),
detail_rows_enrollments AS (
    -- One row per enrollment
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.term,
        s.score,
        s.enrolled_at,
        s.grade_status_or_unknown,
        e.id AS enrollment_id,
        e.student_id AS enrollment_student_id,
        e.term AS enrollment_term,
        e.score AS enrollment_score
    FROM filtered_enrollments s
    LEFT JOIN enrollments e ON e.course_id = s.course_id
),
labeled AS (
    -- Derive helper columns
    SELECT
        *,
        CONCAT(term, '#', CAST(student_id AS STRING)) AS term_ref,
        DATE_TRUNC('HOUR', enrolled_at) AS enrolled_at_hour
    FROM detail_rows_enrollments
),
detail_rows AS (
    -- One row per enrollment
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.term,
        s.score,
        s.grade_status_or_unknown,
        s.enrollment_id,
        s.enrollment_student_id,
        s.enrollment_term,
        s.term_ref,
        s.enrolled_at_hour,
        e.id AS enrollment_id_2,
        e.score AS enrollment_score_2,
        e.grade_status AS enrollment_grade_status,
        e.student_id AS enrollment_student_id_2,
        e.enrolled_at AS enrollment_enrolled_at
    FROM labeled s
    JOIN enrollments e ON e.course_id = s.course_id
),
scored AS (
    -- Label and score rows
    SELECT *, COALESCE(score, 0.0) AS score_filled
    FROM detail_rows
),
with_children_enrollments AS (
    -- One row per enrollment
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.grade_status_or_unknown,
        s.enrollment_id,
        s.enrollment_student_id,
        s.enrolled_at_hour,
        s.enrollment_id_2,
        s.enrollment_score_2,
        s.enrollment_grade_status,
        s.enrollment_student_id_2,
        s.score_filled,
        e.id AS enrollment_id_3,
        e.grade_status AS enrollment_grade_status_2,
        e.enrolled_at AS enrollment_enrolled_at_2,
        e.score AS enrollment_score,
        e.student_id AS enrollment_student_id_3
    FROM scored s
    LEFT JOIN enrollments e ON e.course_id = s.course_id AND e.score < 74.9
),
joined_course AS (
    -- Join courses
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.grade_status_or_unknown,
        s.enrollment_id,
        s.enrollment_student_id,
        s.enrollment_id_2,
        s.enrollment_score_2,
        s.enrollment_student_id_2,
        s.enrollment_id_3,
        s.enrollment_enrolled_at_2,
        s.enrollment_score,
        s.enrollment_student_id_3,
        c.dept AS course_dept,
        c.credits AS course_credits,
        c.course_code
    FROM with_children_enrollments s
    JOIN courses c ON c.id = s.course_id
)
SELECT id, enrollment_id, metric, value
FROM (
    SELECT id, enrollment_id, enrollment_score, enrollment_score_2
    FROM joined_course
) src
UNPIVOT (
    value FOR metric IN (enrollment_score, enrollment_score_2)
) unp
ORDER BY id, enrollment_id, metric
