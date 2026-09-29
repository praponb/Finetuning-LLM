WITH valid_enrollments AS (
    -- Scope the enrollments rows
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        DATEDIFF(DATE '2026-01-01', enrolled_at) AS days_since_enrolled_at
    FROM enrollments
    WHERE grade_status IS NOT NULL
),
scored AS (
    -- Label and score rows
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        ROUND(days_since_enrolled_at / NULLIF(score, 0), 3) AS days_since_enrolled_at_per_score
    FROM valid_enrollments
),
enrollment_stats AS (
    -- Per-course enrollments metrics
    SELECT
        course_id,
        COUNT(*) FILTER (WHERE term <> '2024T1') AS enrollment_term_count,
        COUNT(*) AS enrollment_count
    FROM enrollments
    GROUP BY course_id
),
profiled AS (
    -- Attach enrollments metrics
    SELECT
        s.*,
        COALESCE(e.enrollment_term_count, 0) AS enrollment_term_count,
        COALESCE(e.enrollment_count, 0) AS enrollment_count
    FROM scored s
    LEFT JOIN enrollment_stats e ON e.course_id = s.course_id
)
SELECT
    enrollment_count,
    student_id,
    enrollment_term_count,
    SUM(CASE WHEN term = '2025T2' THEN 1 ELSE 0 END) AS n_term_2025t2,
    COUNT(DISTINCT student_id) AS distinct_student_ids,
    SUM(enrollment_count) FILTER (WHERE grade_status IS NOT NULL) AS enrollment_count_grade_status_is_not_null
FROM profiled
GROUP BY CUBE(enrollment_count, student_id, enrollment_term_count)
ORDER BY enrollment_count NULLS LAST, student_id NULLS LAST, enrollment_term_count NULLS LAST
