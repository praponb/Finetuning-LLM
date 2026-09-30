WITH prepared_enrollments AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        student_id,
        course_id,
        term,
        grade_status,
        enrolled_at,
        CONCAT(grade_status, '#', CAST(student_id AS STRING)) AS grade_status_ref,
        ROUND(score * 1.5, 2) AS score_adj
    FROM enrollments
),
with_parent_course AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.term,
        s.grade_status,
        s.score_adj,
        c.credits AS course_credits,
        c.dept AS course_dept,
        c.level AS course_level
    FROM prepared_enrollments s
    JOIN courses c ON c.id = s.course_id
)
SELECT
    COALESCE(term, 'ALL') AS term,
    COALESCE(course_level, 'ALL') AS course_level,
    MIN(course_credits) AS min_course_credits
FROM with_parent_course
GROUP BY GROUPING SETS ((term, course_level), (term), (course_level), ())
