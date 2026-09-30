WITH filtered_enrollments AS (
    -- Scope the enrollments rows
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        enrolled_at
    FROM enrollments
    WHERE enrolled_at < TIMESTAMP '2025-03-08 09:00:00'
),
totals AS (
    -- Summarise per group
    SELECT
        course_id,
        TO_DATE(enrolled_at) AS enrolled_at_day,
        MAX(enrolled_at) AS last_enrolled_at,
        MAX(score) AS max_score
    FROM filtered_enrollments
    GROUP BY course_id, TO_DATE(enrolled_at)
    HAVING AVG(score) > 30.0
),
filtered AS (
    -- Rows with no matching enrollments
    SELECT s.*
    FROM totals s
    WHERE NOT EXISTS (
            SELECT 1
            FROM enrollments e
            WHERE e.course_id = s.course_id
              AND e.score BETWEEN 8.7 AND 87.8
              AND e.grade_status IN ('PASS', 'INCOMPLETE')
        )
),
enrollment_stats AS (
    -- Per-course enrollments metrics
    SELECT
        course_id,
        COUNT(*) FILTER (WHERE term IN ('2025T2', '2024T2', '2025T1')) AS enrollment_term_count,
        MAX(enrolled_at) AS last_enrollment_enrolled_at,
        MIN(score) AS min_enrollment_score
    FROM enrollments
    GROUP BY course_id
),
summarized_rows AS (
    -- Attach enrollments metrics
    SELECT
        s.*,
        COALESCE(e.enrollment_term_count, 0) AS enrollment_term_count,
        e.last_enrollment_enrolled_at,
        e.min_enrollment_score
    FROM filtered s
    LEFT JOIN enrollment_stats e ON e.course_id = s.course_id
)
-- Native SQL UNPIVOT (wide to long)
SELECT course_id, enrolled_at_day, field, value
FROM (
    SELECT course_id, enrolled_at_day, min_enrollment_score, max_score
    FROM summarized_rows
) src
UNPIVOT INCLUDE NULLS (
    value FOR field IN (min_enrollment_score, max_score)
) unp
