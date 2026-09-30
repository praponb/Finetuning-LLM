WITH prepared_enrollments AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        LOWER(REGEXP_REPLACE(TRIM(grade_status), ' ', '_')) AS grade_status_slug
    FROM enrollments
    WHERE TO_DATE(enrolled_at) < DATE '2025-02-24'
      AND grade_status IS NOT NULL
),
enrollment_stats AS (
    -- Per-course enrollments metrics
    SELECT
        course_id,
        COUNT(*) AS enrollment_count,
        MAX(enrolled_at) AS last_enrollment_enrolled_at
    FROM enrollments
    GROUP BY course_id
),
profiled AS (
    -- Attach enrollments metrics
    SELECT
        s.*,
        COALESCE(e.enrollment_count, 0) AS enrollment_count,
        e.last_enrollment_enrolled_at
    FROM prepared_enrollments s
    LEFT JOIN enrollment_stats e ON e.course_id = s.course_id
),
derived AS (
    -- Label and score rows
    SELECT
        *,
        CASE WHEN enrollment_count >= 5 THEN 'large' WHEN enrollment_count >= 3 THEN 'medium' ELSE 'small' END AS enrollment_count_tier,
        GREATEST(score, enrollment_count) AS greatest_score_enrollment_count
    FROM profiled
    WHERE grade_status IN ('INCOMPLETE', 'WITHDRAWN')
)
SELECT
    enrollment_count,
    student_id,
    course_id,
    GROUPING_ID() AS gid,
    MIN(last_enrollment_enrolled_at) AS first_last_enrollment_enrolled_at,
    ARRAY_SORT(COLLECT_SET(term)) AS term_list
FROM derived
GROUP BY CUBE(enrollment_count, student_id, course_id)
ORDER BY enrollment_count NULLS LAST, student_id NULLS LAST, course_id NULLS LAST
