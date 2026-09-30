WITH filtered AS (
    -- Scope the enrollments rows
    SELECT
        id,
        student_id,
        course_id,
        term,
        grade_status,
        enrolled_at
    FROM enrollments
    WHERE score > 8.7
),
with_parent AS (
    -- Attach course attributes
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.term,
        s.grade_status,
        s.enrolled_at,
        c.course_code,
        c.level AS course_level
    FROM filtered s
    LEFT JOIN courses c ON c.id = s.course_id
),
scored AS (
    -- Add calculated columns
    SELECT
        id,
        student_id,
        course_id,
        grade_status,
        LOWER(TRIM(course_code)) AS course_code_clean
    FROM with_parent
)
-- Subtotals with GROUPING SETS
SELECT
    COALESCE(grade_status, 'ALL') AS grade_status,
    course_id,
    student_id,
    COUNT(DISTINCT course_id) AS distinct_course_ids
FROM scored
GROUP BY GROUPING SETS ((grade_status, course_id, student_id), (grade_status), ())
ORDER BY grade_status NULLS LAST, course_id NULLS LAST, student_id NULLS LAST
