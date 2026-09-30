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
        CONCAT(term, '#', CAST(student_id AS STRING)) AS term_ref
    FROM enrollments
    WHERE grade_status = 'WITHDRAWN'
),
tagged AS (
    -- Add calculated columns
    SELECT *, LENGTH(TRIM(grade_status)) AS grade_status_len
    FROM scoped
    WHERE score > 38.1
),
profiled AS (
    -- Attach enrollments metrics
    SELECT
        s.*,
        (SELECT MAX(e.enrolled_at) FROM enrollments e WHERE e.student_id = s.student_id) AS last_enrollment_enrolled_at
    FROM tagged s
),
qualified AS (
    -- Keep rows whose course matches
    SELECT s.*
    FROM profiled s
    WHERE EXISTS (
            SELECT 1
            FROM courses c
            WHERE c.id = s.course_id
              AND c.dept IN ('CS', 'Bio')
              AND c.credits <= 4
        )
)
SELECT
    id,
    student_id,
    course_id,
    enrolled_at,
    term_ref,
    grade_status_len,
    LENGTH(TRIM(term_ref)) AS term_ref_len
FROM qualified
ORDER BY enrolled_at DESC
