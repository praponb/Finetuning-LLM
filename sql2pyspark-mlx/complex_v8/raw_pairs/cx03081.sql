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
    WHERE grade_status = 'PASS'
),
with_counts AS (
    -- Attach enrollments metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FROM enrollments e WHERE e.student_id = s.student_id) AS enrollment_count
    FROM valid s
)
SELECT
    id,
    course_id,
    term,
    grade_status,
    enrollment_count
FROM with_counts
ORDER BY id DESC NULLS FIRST, term DESC
LIMIT 20
