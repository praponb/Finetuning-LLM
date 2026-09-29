WITH scoped AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status
    FROM enrollments
    WHERE course_id <= 3
),
enrollment_stats AS (
    -- Per-student enrollments metrics
    SELECT
        student_id,
        MAX(enrolled_at) AS last_enrollment_enrolled_at,
        MAX(enrolled_at) AS last_enrollment_enrolled_at_2
    FROM enrollments
    GROUP BY student_id
),
summarized_rows AS (
    -- Attach enrollments metrics
    SELECT
        s.*,
        e.last_enrollment_enrolled_at,
        e.last_enrollment_enrolled_at_2
    FROM scoped s
    LEFT JOIN enrollment_stats e ON e.student_id = s.student_id
),
enrollment_stats_2 AS (
    -- Per-course enrollments metrics
    SELECT
        course_id,
        MAX(enrolled_at) AS last_enrollment_enrolled_at_3,
        COUNT(*) AS enrollment_count
    FROM enrollments
    GROUP BY course_id
),
summarized_rows_2 AS (
    -- Attach enrollments metrics
    SELECT
        s.*,
        e.last_enrollment_enrolled_at_3,
        COALESCE(e.enrollment_count, 0) AS enrollment_count
    FROM summarized_rows s
    LEFT JOIN enrollment_stats_2 e ON e.course_id = s.course_id
),
profiled AS (
    -- Attach enrollments metrics
    SELECT
        s.*,
        (SELECT SUM(e.score) FROM enrollments e WHERE e.student_id = s.student_id) AS total_enrollment_score,
        (SELECT MIN(e.enrolled_at) FROM enrollments e WHERE e.student_id = s.student_id) AS first_enrollment_enrolled_at,
        (SELECT SUM(e.score) FROM enrollments e WHERE e.student_id = s.student_id) AS total_enrollment_score_2
    FROM summarized_rows_2 s
)
-- Subtotals with GROUPING SETS
SELECT
    enrollment_count,
    course_id,
    student_id,
    COUNT(*) FILTER (WHERE term IS NOT NULL) AS n_term_is_not_null
FROM profiled
GROUP BY GROUPING SETS ((enrollment_count, course_id, student_id), (enrollment_count))
ORDER BY enrollment_count NULLS LAST, course_id NULLS LAST, student_id NULLS LAST
