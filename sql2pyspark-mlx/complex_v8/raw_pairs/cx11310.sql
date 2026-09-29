WITH cleaned_courses AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        course_code,
        dept,
        credits,
        level
    FROM courses
    WHERE level IN ('advanced', 'intro')
      AND dept <> 'Art'
),
without_matches AS (
    -- Rows with no matching enrollments
    SELECT s.*
    FROM cleaned_courses s
    WHERE NOT EXISTS (
            SELECT 1
            FROM enrollments e
            WHERE e.course_id = s.id
              AND e.grade_status IN ('INCOMPLETE', 'FAIL', 'PASS')
              AND e.student_id >= 6
        )
      AND s.credits < 3
),
enrollment_stats AS (
    -- Per-course enrollments metrics
    SELECT
        course_id,
        SUM(score) AS total_enrollment_score,
        MAX(enrolled_at) AS last_enrollment_enrolled_at,
        MAX(enrolled_at) AS last_enrollment_enrolled_at_2
    FROM enrollments
    GROUP BY course_id
),
profiled AS (
    -- Attach enrollments metrics
    SELECT
        s.*,
        e.total_enrollment_score,
        e.last_enrollment_enrolled_at,
        e.last_enrollment_enrolled_at_2
    FROM without_matches s
    LEFT JOIN enrollment_stats e ON e.course_id = s.id
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT dept, last_enrollment_enrolled_at_2, level, credits
    FROM profiled
) src
PIVOT (
    SUM(credits)
    FOR level IN ('advanced', 'core')
)
