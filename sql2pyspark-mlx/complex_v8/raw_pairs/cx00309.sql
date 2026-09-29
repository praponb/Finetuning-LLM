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
        ROUND(score * 1.07, 2) AS score_adj,
        CONCAT_WS(': ', term, grade_status) AS term_grade_status_label
    FROM enrollments
),
exploded_detail_enrollments AS (
    -- One row per enrollment
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.score,
        s.grade_status,
        s.score_adj,
        e.id AS enrollment_id,
        e.grade_status AS enrollment_grade_status,
        e.enrolled_at AS enrollment_enrolled_at,
        e.course_id AS enrollment_course_id,
        e.score AS enrollment_score
    FROM scoped_enrollments s
    JOIN enrollments e ON e.student_id = s.student_id
)
SELECT *
FROM (
    SELECT course_id, enrollment_course_id, enrollment_grade_status, enrollment_score
    FROM exploded_detail_enrollments
) src
PIVOT (
    SUM(enrollment_score)
    FOR enrollment_grade_status IN ('PASS', 'WITHDRAWN', 'INCOMPLETE', 'FAIL')
)
ORDER BY course_id, enrollment_course_id
