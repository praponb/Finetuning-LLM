WITH base AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        CONCAT_WS(' / ', grade_status, term) AS grade_status_term_label
    FROM enrollments
    WHERE student_id >= 17
      AND MONTH(enrolled_at) IN (2, 3)
),
with_counts AS (
    -- Attach enrollments metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FROM enrollments e WHERE e.student_id = s.student_id) AS enrollment_count,
        (SELECT COUNT(*) FILTER (WHERE e.grade_status <> 'PASS') FROM enrollments e WHERE e.student_id = s.student_id) AS enrollment_grade_status_count
    FROM base s
)
SELECT *
FROM (
    SELECT enrollment_count, student_id, grade_status, score
    FROM with_counts
) src
PIVOT (
    MIN(score)
    FOR grade_status IN ('PASS', 'INCOMPLETE', 'FAIL')
)
