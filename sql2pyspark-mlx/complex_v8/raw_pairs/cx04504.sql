WITH scoped AS (
    -- Filter enrollments
    SELECT
        id,
        student_id,
        course_id,
        term,
        grade_status,
        enrolled_at,
        DAYOFWEEK(enrolled_at) AS enrolled_at_dayofweek,
        LOWER(TRIM(grade_status)) AS grade_status_clean
    FROM enrollments
),
enrollment_stats AS (
    -- Per-course enrollments metrics
    SELECT
        course_id,
        COUNT(*) FILTER (WHERE grade_status <> 'WITHDRAWN') AS enrollment_grade_status_count,
        MIN(enrolled_at) AS first_enrollment_enrolled_at,
        COUNT(*) AS enrollment_count
    FROM enrollments
    GROUP BY course_id
),
profiled AS (
    -- Attach enrollments metrics
    SELECT
        s.*,
        COALESCE(e.enrollment_grade_status_count, 0) AS enrollment_grade_status_count,
        e.first_enrollment_enrolled_at,
        COALESCE(e.enrollment_count, 0) AS enrollment_count
    FROM scoped s
    LEFT JOIN enrollment_stats e ON e.course_id = s.course_id
),
qualified AS (
    -- Compare against the overall max
    SELECT *
    FROM profiled
    WHERE enrollment_count < (SELECT MAX(enrollment_count) FROM profiled)
),
summarized_rows AS (
    -- Attach enrollments metrics
    SELECT
        s.*,
        (SELECT ROUND(AVG(e.score), 2) FROM enrollments e WHERE e.course_id = s.course_id) AS avg_enrollment_score
    FROM qualified s
)
-- Final projection
SELECT
    id,
    student_id,
    course_id,
    term,
    grade_status,
    enrolled_at_dayofweek,
    enrollment_grade_status_count,
    first_enrollment_enrolled_at,
    enrollment_count,
    avg_enrollment_score,
    HOUR(enrolled_at) AS enrolled_at_hour
FROM summarized_rows
WHERE term <> '2025T1'
