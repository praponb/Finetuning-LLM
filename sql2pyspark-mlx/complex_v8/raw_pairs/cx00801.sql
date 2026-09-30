WITH scoped_enrollments AS (
    -- Scope the enrollments rows
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status
    FROM enrollments
    WHERE grade_status = 'FAIL'
      AND course_id IS NOT NULL
),
with_stats AS (
    -- Attach enrollments metrics
    SELECT
        s.*,
        (SELECT MIN(e.enrolled_at) FROM enrollments e WHERE e.student_id = s.student_id) AS first_enrollment_enrolled_at,
        (SELECT MAX(e.score) FROM enrollments e WHERE e.student_id = s.student_id) AS max_enrollment_score,
        (SELECT COUNT(*) FROM enrollments e WHERE e.student_id = s.student_id) AS enrollment_count
    FROM scoped_enrollments s
),
enriched AS (
    -- Label and score rows
    SELECT
        *,
        GREATEST(score, max_enrollment_score) AS greatest_score_max_enrollment_score
    FROM with_stats
),
profiled AS (
    -- Attach enrollments metrics
    SELECT
        s.*,
        (SELECT SUM(e.score) FROM enrollments e WHERE e.course_id = s.course_id) AS total_enrollment_score,
        (SELECT COUNT(*) FROM enrollments e WHERE e.course_id = s.course_id) AS enrollment_count_2
    FROM enriched s
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT course_id, grade_status, enrollment_count_2
    FROM profiled
) src
PIVOT (
    MAX(enrollment_count_2)
    FOR grade_status IN ('PASS', 'FAIL')
)
ORDER BY course_id
