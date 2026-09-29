WITH scoped_courses AS (
    -- Scope the courses rows
    SELECT
        id,
        course_code,
        dept,
        level
    FROM courses
),
enrollment_stats AS (
    -- Per-course enrollments metrics
    SELECT
        course_id,
        MAX(enrolled_at) AS last_enrollment_enrolled_at,
        COUNT(*) AS enrollment_count,
        COUNT(CASE WHEN term <> '2024T1' THEN 1 END) AS enrollment_term_count
    FROM enrollments
    GROUP BY course_id
),
profiled AS (
    -- Attach enrollments metrics
    SELECT
        s.*,
        e.last_enrollment_enrolled_at,
        COALESCE(e.enrollment_count, 0) AS enrollment_count,
        COALESCE(e.enrollment_term_count, 0) AS enrollment_term_count
    FROM scoped_courses s
    LEFT JOIN enrollment_stats e ON e.course_id = s.id
),
enrollment_stats_2 AS (
    -- Per-course enrollments metrics
    SELECT
        course_id,
        COUNT(*) AS enrollment_count_2,
        MIN(enrolled_at) AS first_enrollment_enrolled_at
    FROM enrollments
    GROUP BY course_id
),
profiled_2 AS (
    -- Attach enrollments metrics
    SELECT
        s.*,
        COALESCE(e.enrollment_count_2, 0) AS enrollment_count_2,
        e.first_enrollment_enrolled_at
    FROM profiled s
    LEFT JOIN enrollment_stats_2 e ON e.course_id = s.id
),
enrollment_stats_3 AS (
    -- Per-course enrollments metrics
    SELECT
        course_id,
        COUNT(*) AS enrollment_count_3,
        MAX(enrolled_at) AS last_enrollment_enrolled_at_2,
        COUNT(*) AS enrollment_count_4
    FROM enrollments
    GROUP BY course_id
),
profiled_3 AS (
    -- Attach enrollments metrics
    SELECT
        s.*,
        COALESCE(e.enrollment_count_3, 0) AS enrollment_count_3,
        e.last_enrollment_enrolled_at_2,
        COALESCE(e.enrollment_count_4, 0) AS enrollment_count_4
    FROM profiled_2 s
    LEFT JOIN enrollment_stats_3 e ON e.course_id = s.id
),
annotated AS (
    -- Derive helper columns
    SELECT
        *,
        SUBSTRING(TRIM(level), 1, 2) AS level_prefix,
        ABS(enrollment_count_2 - 3) AS enrollment_count_2_distance
    FROM profiled_3
),
with_counts AS (
    -- Attach enrollments metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FROM enrollments e WHERE e.course_id = s.id) AS enrollment_count_5,
        (SELECT MIN(e.enrolled_at) FROM enrollments e WHERE e.course_id = s.id) AS first_enrollment_enrolled_at_2
    FROM annotated s
),
derived AS (
    -- Derive helper columns
    SELECT
        id,
        course_code,
        dept,
        level,
        last_enrollment_enrolled_at,
        enrollment_term_count,
        enrollment_count_2,
        first_enrollment_enrolled_at,
        enrollment_count_3,
        last_enrollment_enrolled_at_2,
        enrollment_count_4,
        enrollment_count_5,
        LENGTH(TRIM(level)) AS level_len,
        enrollment_count_4 - enrollment_term_count AS enrollment_count_4_minus_enrollment_term_count,
        COALESCE(level, 'UNKNOWN') AS level_or_unknown
    FROM with_counts
)
SELECT *
FROM (
    SELECT level_len, dept, enrollment_count_3
    FROM derived
) src
PIVOT (
    MAX(enrollment_count_3)
    FOR dept IN ('Art', 'Bio', 'CS', 'Biz')
)
ORDER BY level_len
