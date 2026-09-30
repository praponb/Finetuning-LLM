WITH base_enrollments AS (
    -- Scope the enrollments rows
    SELECT
        id,
        student_id,
        course_id,
        score,
        grade_status,
        enrolled_at
    FROM enrollments
    WHERE (student_id IS NOT NULL OR enrolled_at >= TIMESTAMP '2025-01-07 11:00:00')
),
with_parent_student AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.score,
        s.grade_status,
        s.enrolled_at,
        st.student_name
    FROM base_enrollments s
    JOIN students st ON st.id = s.student_id AND st.gpa <= 3.4
),
grouped AS (
    -- Summarise per group
    SELECT
        DATE_TRUNC('MONTH', enrolled_at) AS enrolled_at_month,
        course_id,
        ARRAY_SORT(COLLECT_SET(grade_status)) AS grade_status_list,
        COUNT(*) FILTER (WHERE grade_status = 'PASS') AS n_grade_status_pass,
        ROUND(100 * COUNT(CASE WHEN UPPER(TRIM(student_name)) LIKE 'G%' THEN 1 END) / COUNT(*), 1) AS pct_upper_trim_student_name_like,
        ROUND(SUM(score), 2) AS total_score,
        SUM(CASE WHEN grade_status NOT IN ('WITHDRAWN', 'INCOMPLETE') THEN 1 ELSE 0 END) AS n_grade_status_not_in_withdraw
    FROM with_parent_student
    GROUP BY 1, 2
),
enrollment_stats AS (
    -- Per-course enrollments metrics
    SELECT course_id, COUNT(*) AS enrollment_count
    FROM enrollments
    GROUP BY course_id
),
with_stats AS (
    -- Attach enrollments metrics
    SELECT
        s.*,
        COALESCE(e.enrollment_count, 0) AS enrollment_count
    FROM grouped s
    LEFT JOIN enrollment_stats e ON e.course_id = s.course_id
),
enrollment_stats_2 AS (
    -- Per-course enrollments metrics
    SELECT course_id, MIN(enrolled_at) AS first_enrollment_enrolled_at
    FROM enrollments
    GROUP BY course_id
),
with_stats_2 AS (
    -- Attach enrollments metrics
    SELECT
        s.*,
        e.first_enrollment_enrolled_at
    FROM with_stats s
    LEFT JOIN enrollment_stats_2 e ON e.course_id = s.course_id
),
with_details AS (
    -- Attach course attributes
    SELECT
        s.enrolled_at_month,
        s.course_id,
        s.n_grade_status_pass,
        s.pct_upper_trim_student_name_like,
        s.total_score,
        s.n_grade_status_not_in_withdraw,
        s.enrollment_count,
        c.dept AS course_dept,
        c.credits AS course_credits,
        c.level AS course_level
    FROM with_stats_2 s
    JOIN courses c ON c.id = s.course_id
)
SELECT
    enrolled_at_month,
    course_id,
    n_grade_status_pass,
    pct_upper_trim_student_name_like,
    total_score,
    n_grade_status_not_in_withdraw,
    enrollment_count,
    course_dept,
    course_credits,
    DATE_TRUNC('DAY', enrolled_at_month) AS enrolled_at_month_day,
    UPPER(TRIM(course_level)) AS course_level_clean
FROM with_details
WHERE n_grade_status_not_in_withdraw <= 8
