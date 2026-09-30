WITH valid AS (
    -- Filter enrollments
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at
    FROM enrollments
    WHERE TO_DATE(enrolled_at) < DATE '2025-03-08'
      AND term IN ('2024T2', '2025T2', '2025T1')
),
with_details AS (
    -- Join courses
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.term,
        s.grade_status,
        c.course_code,
        c.dept AS course_dept,
        c.level AS course_level
    FROM valid s
    JOIN courses c ON c.id = s.course_id
    WHERE c.dept IS NOT NULL
),
labeled AS (
    -- Add calculated columns
    SELECT
        *,
        CASE WHEN course_dept IN ('Bio', 'CS') THEN 'group_a' WHEN course_dept = 'Math' THEN 'group_b' ELSE 'other' END AS course_dept_group
    FROM with_details
)
-- Final projection
SELECT
    id,
    student_id,
    course_id,
    grade_status,
    course_dept,
    course_level,
    course_dept_group,
    LOWER(REGEXP_REPLACE(TRIM(course_dept_group), ' ', '_')) AS course_dept_group_slug,
    COALESCE(term, 'UNKNOWN') AS term_or_unknown
FROM labeled
ORDER BY student_id DESC NULLS FIRST, course_dept_group_slug NULLS LAST
