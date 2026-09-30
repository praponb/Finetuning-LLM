WITH base AS (
    -- Scope the enrollments rows
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        DATE_TRUNC('MONTH', enrolled_at) AS enrolled_at_month,
        ABS(score - 69.6) AS score_distance
    FROM enrollments
    WHERE term = '2025T1'
      AND course_id IS NOT NULL
),
enrollment_stats AS (
    -- Per-student enrollments metrics
    SELECT student_id, MAX(enrolled_at) AS last_enrollment_enrolled_at
    FROM enrollments
    GROUP BY student_id
),
with_counts AS (
    -- Attach enrollments metrics
    SELECT
        s.*,
        e.last_enrollment_enrolled_at
    FROM base s
    LEFT JOIN enrollment_stats e ON e.student_id = s.student_id
),
windowed AS (
    -- Previous / next values per key
    SELECT
        *,
        ROUND(AVG(score_distance) OVER (PARTITION BY DATE_TRUNC('MONTH', enrolled_at_month) ORDER BY enrolled_at ASC, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW), 2) AS score_distance_avg_2,
        ROUND(score_distance / NULLIF(SUM(score_distance) OVER (PARTITION BY MONTH(enrolled_at), course_id), 0), 4) AS score_distance_share_of_enrolled_at_month_course
    FROM with_counts
)
SELECT
    id,
    student_id,
    course_id,
    score,
    grade_status,
    enrolled_at,
    last_enrollment_enrolled_at,
    score_distance_share_of_enrolled_at_month_course,
    CONCAT_WS('|', grade_status, term) AS grade_status_term_label
FROM windowed
