WITH filtered AS (
    -- Scope the courses rows
    SELECT *
    FROM courses
    WHERE level IS NOT NULL
),
profiled AS (
    -- Attach enrollments metrics
    SELECT
        s.*,
        (SELECT COUNT(CASE WHEN e.term IN ('2024T2', '2024T1', '2025T2') THEN 1 END) FROM enrollments e WHERE e.course_id = s.id) AS enrollment_term_count,
        (SELECT COUNT(*) FROM enrollments e WHERE e.course_id = s.id) AS enrollment_count
    FROM filtered s
),
with_activity AS (
    -- Rows that have matching enrollments
    SELECT s.*
    FROM profiled s
    WHERE EXISTS (
            SELECT 1
            FROM enrollments e
            WHERE e.course_id = s.id
              AND e.grade_status <> 'WITHDRAWN'
              AND e.score >= 16.3
        )
),
filtered_2 AS (
    -- Compare against the overall avg
    SELECT *
    FROM with_activity
    WHERE enrollment_term_count > (SELECT AVG(enrollment_term_count) FROM with_activity)
)
-- Native SQL UNPIVOT (wide to long)
SELECT id, metric, value
FROM (
    SELECT id, credits, enrollment_count, enrollment_term_count
    FROM filtered_2
) src
UNPIVOT INCLUDE NULLS (
    value FOR metric IN (credits AS m_credits, enrollment_count AS enrollment_count_v, enrollment_term_count AS m_enrollment_term_count)
) unp
