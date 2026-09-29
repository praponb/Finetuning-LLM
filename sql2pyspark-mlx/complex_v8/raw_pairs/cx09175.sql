WITH base AS (
    -- Filter courses
    SELECT
        id,
        course_code,
        dept,
        credits,
        level,
        CONCAT_WS(' / ', course_code, dept) AS course_code_dept_label,
        LOWER(TRIM(level)) AS level_clean,
        LENGTH(TRIM(level)) AS level_len
    FROM courses
),
detail_rows AS (
    -- One row per enrollment
    SELECT
        s.id,
        s.dept,
        s.level,
        s.course_code_dept_label,
        e.id AS enrollment_id,
        e.term AS enrollment_term,
        e.score AS enrollment_score,
        e.student_id AS enrollment_student_id,
        e.enrolled_at AS enrollment_enrolled_at
    FROM base s
    LEFT JOIN enrollments e ON e.course_id = s.id
),
enrollment_student_id_stats AS (
    -- Avg enrollment_score per enrollment_student_id
    SELECT enrollment_student_id, AVG(enrollment_score) AS avg_enrollment_score
    FROM detail_rows
    GROUP BY enrollment_student_id
),
above_average AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_enrollment_score,
        ROUND(s.enrollment_score - g.avg_enrollment_score, 2) AS enrollment_score_minus_avg
    FROM detail_rows s
    JOIN enrollment_student_id_stats g ON g.enrollment_student_id = s.enrollment_student_id
    WHERE s.enrollment_score > g.avg_enrollment_score
)
SELECT
    enrollment_id,
    enrollment_student_id,
    dept,
    GROUPING_ID() AS grouping_level,
    COUNT(*) FILTER (WHERE level IN ('intro', 'core')) AS n_level_in_intro_core,
    ROUND(SUM(avg_enrollment_score) / NULLIF(SUM(enrollment_score_minus_avg), 0), 3) AS avg_enrollment_score_to_enrollment_score_minus_a
FROM above_average
GROUP BY CUBE(enrollment_id, enrollment_student_id, dept)
ORDER BY enrollment_id NULLS LAST, enrollment_student_id NULLS LAST, dept NULLS LAST
