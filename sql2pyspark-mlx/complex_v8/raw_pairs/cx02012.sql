WITH valid_enrollments AS (
    -- Filter enrollments
    SELECT
        id,
        student_id,
        course_id,
        score,
        grade_status,
        enrolled_at,
        CONCAT_WS(' / ', grade_status, term) AS grade_status_term_label,
        DATEDIFF(DATE '2026-01-01', enrolled_at) AS days_since_enrolled_at,
        ABS(score - 44.4) AS score_distance
    FROM enrollments
    WHERE term = '2024T1'
),
with_parent AS (
    -- Join courses
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.score,
        s.grade_status,
        s.enrolled_at,
        s.grade_status_term_label,
        s.days_since_enrolled_at,
        s.score_distance,
        c.dept AS course_dept,
        c.level AS course_level,
        c.credits AS course_credits
    FROM valid_enrollments s
    JOIN courses c ON c.id = s.course_id
)
SELECT
    student_id,
    course_id,
    COALESCE(grade_status, 'ALL') AS grade_status,
    ROUND(STDDEV(course_credits), 2) AS stddev_course_credits
FROM with_parent
GROUP BY GROUPING SETS ((student_id, course_id, grade_status), (student_id), (course_id))
ORDER BY student_id NULLS LAST, course_id NULLS LAST, grade_status NULLS LAST
