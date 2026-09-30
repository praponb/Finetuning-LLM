WITH prepared AS (
    -- Scope the enrollments rows
    SELECT
        id,
        student_id,
        course_id,
        term,
        grade_status,
        enrolled_at,
        DATEDIFF(DATE '2026-01-01', enrolled_at) AS days_since_enrolled_at
    FROM enrollments
    WHERE course_id IS NOT NULL
),
sequenced AS (
    -- Window metrics over several partitions
    SELECT
        id,
        student_id,
        course_id,
        ROUND(AVG(days_since_enrolled_at) OVER (PARTITION BY grade_status), 2) AS grade_status_avg_days_since_enrolled_at,
        ROUND(days_since_enrolled_at / NULLIF(SUM(days_since_enrolled_at) OVER w_term_student_id, 0), 4) AS days_since_enrolled_at_share_of_term_student_id,
        ROUND((UNIX_TIMESTAMP(enrolled_at) - UNIX_TIMESTAMP(LAG(enrolled_at) OVER (ORDER BY days_since_enrolled_at, id))) / 60, 1) AS minutes_since_prev_enrolled_at,
        ROUND(AVG(days_since_enrolled_at) OVER (PARTITION BY term, student_id), 2) AS term_student_id_avg_days_since_enrolled_at,
        ROUND(days_since_enrolled_at - AVG(days_since_enrolled_at) OVER (PARTITION BY grade_status), 2) AS days_since_enrolled_at_vs_grade_status_avg,
        SUM(days_since_enrolled_at) OVER (PARTITION BY grade_status ORDER BY enrolled_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_days_since_enrolled_at,
        MIN(days_since_enrolled_at) OVER (PARTITION BY grade_status) AS grade_status_min_days_since_enrolled_at,
        ROUND(AVG(days_since_enrolled_at) OVER (PARTITION BY YEAR(enrolled_at) ORDER BY enrolled_at DESC, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW), 2) AS days_since_enrolled_at_avg_2
    FROM prepared
    WINDOW w_term_student_id AS (PARTITION BY term, student_id)
)
SELECT
    id,
    student_id,
    course_id,
    days_since_enrolled_at_share_of_term_student_id,
    minutes_since_prev_enrolled_at,
    term_student_id_avg_days_since_enrolled_at,
    grade_status_min_days_since_enrolled_at,
    days_since_enrolled_at_avg_2
FROM sequenced
ORDER BY term_student_id_avg_days_since_enrolled_at DESC, id
LIMIT 10
