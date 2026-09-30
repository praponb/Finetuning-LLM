WITH scoped AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        FLOOR(score / 25) * 25 AS score_bucket
    FROM enrollments
    WHERE course_id IS NOT NULL
),
line_level AS (
    -- One row per enrollment
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.term,
        s.grade_status,
        s.score_bucket,
        e.id AS enrollment_id,
        e.term AS enrollment_term,
        e.enrolled_at AS enrollment_enrolled_at,
        e.grade_status AS enrollment_grade_status,
        e.course_id AS enrollment_course_id
    FROM scoped s
    JOIN enrollments e ON e.student_id = s.student_id
),
enrollment_grade_status_stats AS (
    -- Max score_bucket per enrollment_grade_status
    SELECT enrollment_grade_status, MAX(score_bucket) AS max_score_bucket
    FROM line_level
    GROUP BY enrollment_grade_status
),
compared AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.max_score_bucket,
        ROUND(s.score_bucket - g.max_score_bucket, 2) AS score_bucket_minus_max
    FROM line_level s
    JOIN enrollment_grade_status_stats g ON g.enrollment_grade_status = s.enrollment_grade_status
    WHERE s.score_bucket = g.max_score_bucket
)
SELECT
    enrollment_grade_status,
    enrollment_term,
    GROUPING(enrollment_grade_status) AS is_total_enrollment_grade_status,
    ROUND(100 * COUNT(CASE WHEN enrollment_grade_status = 'WITHDRAWN' THEN 1 END) / COUNT(*), 1) AS pct_enrollment_grade_status_with
FROM compared
GROUP BY GROUPING SETS ((enrollment_grade_status, enrollment_term), (enrollment_grade_status), (enrollment_term), ())
ORDER BY enrollment_grade_status NULLS LAST, enrollment_term NULLS LAST
