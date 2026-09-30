WITH base_courses AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        dept,
        credits,
        level,
        LOWER(REGEXP_REPLACE(TRIM(level), ' ', '_')) AS level_slug,
        CASE WHEN credits >= 3 THEN 'A' WHEN credits >= 2 THEN 'B' ELSE 'C' END AS credits_tier,
        LOWER(REGEXP_REPLACE(TRIM(course_code), ' ', '_')) AS course_code_slug
    FROM courses
    WHERE credits > 2
),
scored AS (
    -- Add calculated columns
    SELECT
        id,
        dept,
        credits,
        level_slug,
        credits_tier,
        course_code_slug,
        LOWER(REGEXP_REPLACE(TRIM(dept), ' ', '_')) AS dept_slug,
        COALESCE(level, 'UNKNOWN') AS level_or_unknown
    FROM base_courses
),
enrollment_stats AS (
    -- Per-course enrollments metrics
    SELECT
        course_id,
        MAX(enrolled_at) AS last_enrollment_enrolled_at,
        MIN(enrolled_at) AS first_enrollment_enrolled_at
    FROM enrollments
    GROUP BY course_id
),
summarized_rows AS (
    -- Attach enrollments metrics
    SELECT
        s.*,
        e.last_enrollment_enrolled_at,
        e.first_enrollment_enrolled_at
    FROM scored s
    LEFT JOIN enrollment_stats e ON e.course_id = s.id
),
enrollment_stats_2 AS (
    -- Per-course enrollments metrics
    SELECT
        course_id,
        MIN(enrolled_at) AS first_enrollment_enrolled_at_2,
        COUNT(*) AS enrollment_count
    FROM enrollments
    GROUP BY course_id
),
summarized_rows_2 AS (
    -- Attach enrollments metrics
    SELECT
        s.*,
        e.first_enrollment_enrolled_at_2,
        COALESCE(e.enrollment_count, 0) AS enrollment_count
    FROM summarized_rows s
    LEFT JOIN enrollment_stats_2 e ON e.course_id = s.id
),
with_activity AS (
    -- Rows that have matching enrollments
    SELECT s.*
    FROM summarized_rows_2 s
    WHERE EXISTS (
            SELECT 1
            FROM enrollments e
            WHERE e.course_id = s.id
              AND e.student_id <= 7
        )
),
sequenced AS (
    -- Running and moving aggregates
    SELECT
        id,
        dept,
        credits,
        level_slug,
        credits_tier,
        course_code_slug,
        dept_slug,
        first_enrollment_enrolled_at_2,
        ROW_NUMBER() OVER (PARTITION BY credits_tier ORDER BY credits DESC, id) AS seq_no,
        NTILE(5) OVER (PARTITION BY credits_tier ORDER BY credits DESC, id) AS credits_quintile,
        LAG(enrollment_count) OVER (PARTITION BY enrollment_count ORDER BY first_enrollment_enrolled_at_2 DESC, id) AS prev_enrollment_count
    FROM with_activity
),
windowed AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        MIN(seq_no) OVER (PARTITION BY dept, credits_quintile) AS dept_credits_quintile_min_seq_no,
        ROUND(credits_quintile - AVG(credits_quintile) OVER w_dept_credits_quintile, 2) AS credits_quintile_vs_dept_credits_quintile_avg,
        LEAD(prev_enrollment_count) OVER (PARTITION BY TO_DATE(first_enrollment_enrolled_at_2), dept ORDER BY prev_enrollment_count DESC, id) AS next_prev_enrollment_count
    FROM sequenced
    WINDOW w_dept_credits_quintile AS (PARTITION BY dept, credits_quintile)
)
-- Final projection
SELECT
    id,
    dept,
    credits,
    level_slug,
    credits_tier,
    course_code_slug,
    dept_slug,
    first_enrollment_enrolled_at_2,
    seq_no,
    credits_quintile,
    prev_enrollment_count,
    dept_credits_quintile_min_seq_no,
    next_prev_enrollment_count
FROM windowed
ORDER BY level_slug, credits_quintile
