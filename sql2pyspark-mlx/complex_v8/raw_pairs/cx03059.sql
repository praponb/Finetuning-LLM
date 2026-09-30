WITH scoped AS (
    -- Scope the enrollments rows
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        CASE WHEN term IN ('2025T2', '2024T1') THEN 'group_a' WHEN term = '2025T1' THEN 'group_b' ELSE 'other' END AS term_group
    FROM enrollments
    WHERE student_id <= 16
),
scored AS (
    -- Label and score rows
    SELECT
        *,
        CONCAT(term_group, '#', CAST(student_id AS STRING)) AS term_group_ref
    FROM scoped
),
enrollment_stats AS (
    -- Per-course enrollments metrics
    SELECT
        course_id,
        COUNT(*) AS enrollment_count,
        ROUND(AVG(score), 2) AS avg_enrollment_score,
        COUNT(*) FILTER (WHERE grade_status NOT IN ('PASS', 'WITHDRAWN')) AS enrollment_grade_status_count
    FROM enrollments
    GROUP BY course_id
),
with_stats AS (
    -- Attach enrollments metrics
    SELECT
        s.*,
        COALESCE(e.enrollment_count, 0) AS enrollment_count,
        e.avg_enrollment_score,
        COALESCE(e.enrollment_grade_status_count, 0) AS enrollment_grade_status_count
    FROM scored s
    LEFT JOIN enrollment_stats e ON e.course_id = s.course_id
)
SELECT
    grade_status,
    student_id,
    course_id,
    GROUPING_ID() AS grouping_level,
    PERCENTILE_APPROX(enrollment_count, 0.5) AS median_enrollment_count
FROM with_stats
GROUP BY GROUPING SETS ((grade_status, student_id, course_id), (grade_status))
