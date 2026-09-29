WITH recent_courses AS (
    -- Filter courses
    SELECT
        id,
        course_code,
        credits,
        level,
        CASE WHEN level IN ('core', 'intro') THEN 'group_a' WHEN level = 'advanced' THEN 'group_b' ELSE 'other' END AS level_group,
        CASE WHEN credits >= 3 THEN 'HIGH' WHEN credits >= 2 THEN 'MEDIUM' ELSE 'LOW' END AS credits_tier,
        LENGTH(TRIM(course_code)) AS course_code_len
    FROM courses
    WHERE credits > 2
      AND UPPER(TRIM(course_code)) LIKE 'C%'
),
enrollment_stats AS (
    -- Per-course enrollments metrics
    SELECT
        course_id,
        COUNT(*) AS enrollment_count,
        COUNT(*) AS enrollment_count_2,
        COUNT(*) FILTER (WHERE grade_status = 'INCOMPLETE') AS enrollment_grade_status_count
    FROM enrollments
    GROUP BY course_id
),
summarized_rows AS (
    -- Attach enrollments metrics
    SELECT
        s.*,
        COALESCE(e.enrollment_count, 0) AS enrollment_count,
        COALESCE(e.enrollment_count_2, 0) AS enrollment_count_2,
        COALESCE(e.enrollment_grade_status_count, 0) AS enrollment_grade_status_count
    FROM recent_courses s
    LEFT JOIN enrollment_stats e ON e.course_id = s.id
),
annotated AS (
    -- Add calculated columns
    SELECT
        *,
        CONCAT(course_code, '#', CAST(enrollment_count AS STRING)) AS course_code_ref,
        LOWER(REGEXP_REPLACE(TRIM(course_code), ' ', '_')) AS course_code_slug
    FROM summarized_rows
)
SELECT
    id,
    course_code,
    credits,
    level_group,
    credits_tier,
    enrollment_count,
    enrollment_count_2,
    enrollment_grade_status_count,
    course_code_slug
FROM annotated
ORDER BY course_code_slug
