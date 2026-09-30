WITH valid AS (
    -- Filter enrollments
    SELECT
        id,
        student_id,
        course_id,
        score,
        grade_status,
        enrolled_at,
        ABS(score - 59.7) AS score_distance
    FROM enrollments
    WHERE student_id >= 18
      AND course_id IS NOT NULL
),
profiled AS (
    -- Attach enrollments metrics
    SELECT
        s.*,
        (SELECT ROUND(AVG(e.score), 2) FROM enrollments e WHERE e.student_id = s.student_id) AS avg_enrollment_score,
        (SELECT COUNT(*) FROM enrollments e WHERE e.student_id = s.student_id) AS enrollment_count
    FROM valid s
)
-- Subtotals with ROLLUP
SELECT
    enrollment_count,
    COALESCE(grade_status, 'ALL') AS grade_status,
    student_id,
    GROUPING_ID() AS grouping_level,
    MIN(enrolled_at) AS first_enrolled_at,
    COUNT(DISTINCT student_id) AS distinct_student_ids
FROM profiled
GROUP BY ROLLUP(enrollment_count, grade_status, student_id)
ORDER BY enrollment_count NULLS LAST, grade_status NULLS LAST, student_id NULLS LAST
