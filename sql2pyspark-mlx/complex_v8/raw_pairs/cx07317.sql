WITH scoped_courses AS (
    -- Filter courses
    SELECT
        id,
        course_code,
        credits,
        level,
        COALESCE(credits, 0) AS credits_filled,
        LOWER(TRIM(level)) AS level_clean
    FROM courses
    WHERE credits BETWEEN 2 AND 4
),
detail_rows_enrollments AS (
    -- One row per enrollment
    SELECT
        s.id,
        s.level,
        s.credits_filled,
        s.level_clean,
        e.id AS enrollment_id,
        e.student_id AS enrollment_student_id,
        e.grade_status AS enrollment_grade_status,
        e.term AS enrollment_term,
        e.enrolled_at AS enrollment_enrolled_at
    FROM scoped_courses s
    JOIN enrollments e ON e.course_id = s.id
),
enrollment_student_id_stats AS (
    -- Median credits_filled per enrollment_student_id
    SELECT enrollment_student_id, PERCENTILE_APPROX(credits_filled, 0.5) AS median_credits_filled
    FROM detail_rows_enrollments
    GROUP BY enrollment_student_id
),
compared AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.median_credits_filled,
        ROUND(s.credits_filled - g.median_credits_filled, 2) AS credits_filled_minus_median
    FROM detail_rows_enrollments s
    JOIN enrollment_student_id_stats g ON g.enrollment_student_id = s.enrollment_student_id
    WHERE s.credits_filled >= g.median_credits_filled
)
SELECT
    id,
    level,
    credits_filled,
    enrollment_id,
    enrollment_term,
    median_credits_filled,
    credits_filled_minus_median,
    DATE_TRUNC('HOUR', enrollment_enrolled_at) AS enrollment_enrolled_at_hour
FROM compared
ORDER BY id DESC NULLS FIRST
