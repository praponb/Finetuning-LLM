WITH recent AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        DATEDIFF(DATE '2026-01-01', enrolled_at) AS days_since_enrolled_at,
        HOUR(enrolled_at) AS enrolled_at_hour
    FROM enrollments
    WHERE grade_status = 'PASS'
),
totals AS (
    -- Aggregate with conditional metrics
    SELECT
        course_id,
        TO_DATE(enrolled_at) AS enrolled_at_day,
        grade_status,
        SUM(CASE WHEN term = '2024T1' THEN 1 ELSE 0 END) AS n_term_2024t1,
        ROUND(SUM(days_since_enrolled_at) / NULLIF(SUM(enrolled_at_hour), 0), 3) AS days_since_enrolled_at_to_enrolled_at_hour_ratio
    FROM recent
    WHERE enrolled_at_hour > 8
    GROUP BY course_id, TO_DATE(enrolled_at), grade_status
),
combined_details AS (
    -- Join courses
    SELECT s.*, c.level AS course_level, c.dept AS course_dept
    FROM totals s
    LEFT JOIN courses c ON c.id = s.course_id
),
with_activity AS (
    -- Compare against the overall avg
    SELECT *
    FROM combined_details
    WHERE days_since_enrolled_at_to_enrolled_at_hour_ratio > (SELECT AVG(days_since_enrolled_at_to_enrolled_at_hour_ratio) FROM combined_details)
),
combined_details_course AS (
    -- Enrich with dimension data
    SELECT s.*, c.credits AS course_credits
    FROM with_activity s
    JOIN courses c ON c.id = s.course_id
),
enrollment_stats AS (
    -- Per-course enrollments metrics
    SELECT course_id, MAX(enrolled_at) AS last_enrollment_enrolled_at
    FROM enrollments
    GROUP BY course_id
),
with_counts AS (
    -- Attach enrollments metrics
    SELECT
        s.*,
        e.last_enrollment_enrolled_at
    FROM combined_details_course s
    LEFT JOIN enrollment_stats e ON e.course_id = s.course_id
),
joined AS (
    -- Join courses
    SELECT
        s.course_id,
        s.enrolled_at_day,
        s.grade_status,
        s.days_since_enrolled_at_to_enrolled_at_hour_ratio,
        s.course_level,
        s.course_dept,
        s.course_credits,
        c.course_code
    FROM with_counts s
    LEFT JOIN courses c ON c.id = s.course_id
)
-- Final projection
SELECT
    course_id,
    days_since_enrolled_at_to_enrolled_at_hour_ratio,
    course_level,
    course_credits,
    course_code,
    COALESCE(course_credits, 0) AS course_credits_filled,
    QUARTER(enrolled_at_day) AS enrolled_at_day_quarter
FROM joined
WHERE course_dept IS NOT NULL
ORDER BY enrolled_at_day_quarter
