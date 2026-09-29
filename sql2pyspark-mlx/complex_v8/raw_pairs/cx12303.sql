WITH base AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        CASE WHEN grade_status IN ('PASS', 'FAIL') THEN 'group_a' WHEN grade_status = 'WITHDRAWN' THEN 'group_b' ELSE 'other' END AS grade_status_group,
        MONTH(enrolled_at) AS enrolled_at_month,
        COALESCE(grade_status, 'UNKNOWN') AS grade_status_or_unknown
    FROM enrollments
    WHERE student_id IS NOT NULL
),
filtered AS (
    -- Rows with no matching enrollments
    SELECT s.*
    FROM base s
    WHERE NOT EXISTS (
            SELECT 1
            FROM enrollments e
            WHERE e.course_id = s.course_id
              AND e.student_id >= 8
              AND e.grade_status <> 'WITHDRAWN'
        )
),
combined_details_course AS (
    -- Join courses
    SELECT s.*, c.level AS course_level
    FROM filtered s
    LEFT JOIN courses c ON c.id = s.course_id
),
grade_status_group_stats AS (
    -- Avg enrolled_at_month per grade_status_group
    SELECT grade_status_group, AVG(enrolled_at_month) AS avg_enrolled_at_month
    FROM combined_details_course
    GROUP BY grade_status_group
),
compared AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_enrolled_at_month,
        ROUND(s.enrolled_at_month - g.avg_enrolled_at_month, 2) AS enrolled_at_month_minus_avg
    FROM combined_details_course s
    JOIN grade_status_group_stats g ON g.grade_status_group = s.grade_status_group
    WHERE s.enrolled_at_month > g.avg_enrolled_at_month
),
derived AS (
    -- Add calculated columns
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        grade_status_group,
        enrolled_at_month,
        grade_status_or_unknown,
        course_level,
        avg_enrolled_at_month,
        enrolled_at_month_minus_avg,
        CASE WHEN enrolled_at_month >= 9 THEN 'A' WHEN enrolled_at_month >= 4 THEN 'B' ELSE 'C' END AS enrolled_at_month_tier,
        LENGTH(TRIM(grade_status_group)) AS grade_status_group_len
    FROM compared
    WHERE grade_status_group IN ('group_a', 'other')
)
SELECT
    id,
    student_id,
    course_id,
    term,
    score,
    grade_status,
    grade_status_group,
    enrolled_at_month,
    grade_status_or_unknown,
    course_level,
    avg_enrolled_at_month,
    enrolled_at_month_minus_avg,
    ROUND(enrolled_at_month / NULLIF(avg_enrolled_at_month, 0), 3) AS enrolled_at_month_per_avg_enrolled_at_month,
    CONCAT(course_level, '#', CAST(enrolled_at_month AS STRING)) AS course_level_ref
FROM derived
ORDER BY id
