WITH scoped AS (
    -- Filter enrollments
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        DATE_FORMAT(enrolled_at, 'MMM yyyy') AS enrolled_at_label
    FROM enrollments
    WHERE grade_status IN ('FAIL', 'WITHDRAWN')
),
annotated AS (
    -- Label and score rows
    SELECT
        id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        enrolled_at_label,
        LENGTH(TRIM(grade_status)) AS grade_status_len
    FROM scoped
),
with_details_course AS (
    -- Join courses
    SELECT
        s.id,
        s.course_id,
        s.term,
        s.score,
        s.grade_status,
        s.enrolled_at,
        s.enrolled_at_label,
        s.grade_status_len,
        c.course_code,
        c.dept AS course_dept
    FROM annotated s
    JOIN courses c ON c.id = s.course_id
),
with_parent_course AS (
    -- Enrich with dimension data
    SELECT
        s.*,
        c.level AS course_level,
        c.dept AS course_dept_2,
        c.credits AS course_credits
    FROM with_details_course s
    JOIN courses c ON c.id = s.course_id
)
-- Final projection
SELECT
    id,
    term,
    grade_status,
    enrolled_at,
    enrolled_at_label,
    course_code,
    course_dept,
    course_level,
    course_dept_2,
    course_credits,
    DATEDIFF(DATE '2025-06-30', enrolled_at) AS days_since_enrolled_at
FROM with_parent_course
WHERE course_level NOT IN ('advanced', 'intro')
ORDER BY course_code DESC
