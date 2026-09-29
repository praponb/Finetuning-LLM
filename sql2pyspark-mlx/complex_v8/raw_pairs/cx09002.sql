WITH prepared_courses AS (
    -- Filter courses
    SELECT
        id,
        course_code,
        dept,
        credits
    FROM courses
    WHERE dept = 'Art'
),
exploded_detail_enrollments AS (
    -- One row per enrollment
    SELECT
        s.id,
        s.dept,
        s.credits,
        e.id AS enrollment_id,
        e.enrolled_at AS enrollment_enrolled_at,
        e.grade_status AS enrollment_grade_status
    FROM prepared_courses s
    LEFT JOIN enrollments e ON e.course_id = s.id
),
annotated AS (
    -- Derive helper columns
    SELECT
        *,
        CASE WHEN credits >= 4 THEN 'large' WHEN credits >= 2 THEN 'medium' ELSE 'small' END AS credits_tier,
        LENGTH(TRIM(dept)) AS dept_len
    FROM exploded_detail_enrollments
    WHERE dept = 'Math'
),
enriched AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.credits,
        s.enrollment_id,
        s.enrollment_enrolled_at,
        s.enrollment_grade_status,
        s.credits_tier,
        s.dept_len,
        e.grade_status AS enrollment_grade_status_2
    FROM annotated s
    LEFT JOIN enrollments e ON e.id = s.enrollment_id
),
joined AS (
    -- Enrich with dimension data
    SELECT
        s.*,
        e.student_id AS enrollment_student_id,
        e.enrolled_at AS enrollment_enrolled_at_2
    FROM enriched s
    JOIN enrollments e ON e.id = s.enrollment_id
    WHERE TO_DATE(e.enrolled_at) < DATE '2025-02-24'
),
tagged AS (
    -- Derive helper columns
    SELECT
        id,
        credits,
        enrollment_id,
        enrollment_enrolled_at,
        credits_tier,
        dept_len,
        enrollment_enrolled_at_2,
        ROUND(dept_len / NULLIF(credits, 0), 3) AS dept_len_per_credits,
        DATE_TRUNC('WEEK', enrollment_enrolled_at_2) AS enrollment_enrolled_at_2_week
    FROM joined
)
-- Subtotals with GROUPING SETS
SELECT
    credits,
    credits_tier,
    enrollment_id,
    PERCENTILE_APPROX(dept_len_per_credits, 0.5) AS median_dept_len_per_credits
FROM tagged
GROUP BY GROUPING SETS ((credits, credits_tier, enrollment_id), (credits), ())
ORDER BY credits NULLS LAST, credits_tier NULLS LAST, enrollment_id NULLS LAST
