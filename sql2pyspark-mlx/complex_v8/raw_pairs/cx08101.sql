WITH cleaned AS (
    -- Filter enrollments
    SELECT
        id,
        student_id,
        course_id,
        term,
        grade_status,
        enrolled_at,
        QUARTER(enrolled_at) AS enrolled_at_quarter
    FROM enrollments
    WHERE term IS NOT NULL
),
joined_course AS (
    -- Join courses
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.grade_status,
        s.enrolled_at,
        s.enrolled_at_quarter,
        c.level AS course_level,
        c.course_code,
        c.dept AS course_dept
    FROM cleaned s
    JOIN courses c ON c.id = s.course_id
),
annotated AS (
    -- Label and score rows
    SELECT
        *,
        CASE WHEN course_level IN ('intro', 'advanced') THEN 'group_a' WHEN course_level = 'core' THEN 'group_b' ELSE 'other' END AS course_level_group,
        CONCAT_WS(' / ', grade_status, course_dept) AS grade_status_course_dept_label
    FROM joined_course
),
with_parent_course AS (
    -- Attach course attributes
    SELECT s.*, c.dept AS course_dept_2
    FROM annotated s
    JOIN courses c ON c.id = s.course_id
),
labeled AS (
    -- Derive helper columns
    SELECT
        id,
        student_id,
        course_id,
        grade_status,
        enrolled_at_quarter,
        course_level,
        course_code,
        course_dept,
        DATE_FORMAT(enrolled_at, 'yyyy-MM') AS enrolled_at_label,
        COALESCE(grade_status, 'UNKNOWN') AS grade_status_or_unknown
    FROM with_parent_course
)
-- Final projection
SELECT
    id,
    course_id,
    grade_status,
    enrolled_at_quarter,
    course_dept,
    enrolled_at_label,
    COALESCE(grade_status_or_unknown, 'UNKNOWN') AS grade_status_or_unknown_or_unknown,
    ABS(enrolled_at_quarter - 3) AS enrolled_at_quarter_distance
FROM labeled
ORDER BY enrolled_at_label DESC NULLS FIRST, course_dept
