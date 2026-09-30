WITH prepared AS (
    -- Filter enrollments
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        COALESCE(score, 0.0) AS score_filled,
        SUBSTRING(TRIM(term), 1, 4) AS term_prefix
    FROM enrollments
),
enrollment_stats AS (
    -- Per-course enrollments metrics
    SELECT
        course_id,
        COUNT(*) AS enrollment_count,
        COUNT(*) AS enrollment_count_2,
        MAX(enrolled_at) AS last_enrollment_enrolled_at
    FROM enrollments
    GROUP BY course_id
),
summarized_rows AS (
    -- Attach enrollments metrics
    SELECT
        s.*,
        COALESCE(e.enrollment_count, 0) AS enrollment_count,
        COALESCE(e.enrollment_count_2, 0) AS enrollment_count_2,
        e.last_enrollment_enrolled_at
    FROM prepared s
    LEFT JOIN enrollment_stats e ON e.course_id = s.course_id
),
joined_course AS (
    -- Join courses
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.term,
        s.score,
        s.score_filled,
        s.enrollment_count,
        s.last_enrollment_enrolled_at,
        c.level AS course_level
    FROM summarized_rows s
    LEFT JOIN courses c ON c.id = s.course_id
),
course_level_stats AS (
    -- Avg enrollment_count per course_level
    SELECT course_level, AVG(enrollment_count) AS avg_enrollment_count
    FROM joined_course
    GROUP BY course_level
),
above_average AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_enrollment_count,
        ROUND(s.enrollment_count - g.avg_enrollment_count, 2) AS enrollment_count_minus_avg
    FROM joined_course s
    JOIN course_level_stats g ON g.course_level = s.course_level
    WHERE s.enrollment_count > g.avg_enrollment_count
)
-- Final projection
SELECT
    id,
    student_id,
    course_id,
    term,
    score,
    score_filled,
    enrollment_count,
    last_enrollment_enrolled_at,
    course_level,
    avg_enrollment_count,
    enrollment_count_minus_avg
FROM above_average
WHERE course_id >= 5
ORDER BY enrollment_count DESC NULLS FIRST
