WITH filtered AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        UPPER(TRIM(grade_status)) AS grade_status_clean,
        DATE_TRUNC('WEEK', enrolled_at) AS enrolled_at_week
    FROM enrollments
    WHERE course_id <= 3
),
windowed AS (
    -- Previous / next values per key
    SELECT
        *,
        DENSE_RANK() OVER (PARTITION BY grade_status, student_id ORDER BY enrolled_at_week ASC) AS enrolled_at_week_dense_rank,
        LAG(enrolled_at, 2) OVER (ORDER BY enrolled_at_week DESC, id) AS prev2_enrolled_at,
        MAX(score) OVER (PARTITION BY course_id, student_id) AS course_id_student_id_max_score,
        NTILE(4) OVER w_grade_status_student_id_by_e_2 AS enrolled_at_week_quartile,
        ROUND(score - AVG(score) OVER (PARTITION BY course_id, student_id), 2) AS score_vs_course_id_student_id_avg,
        ROUND(PERCENT_RANK() OVER (PARTITION BY grade_status, student_id ORDER BY enrolled_at_week ASC), 3) AS enrolled_at_week_percent_rank,
        MIN(score) OVER (ORDER BY enrolled_at_week, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_score_so_far,
        SUM(score) OVER (PARTITION BY course_id, student_id) AS course_id_student_id_sum_score,
        COUNT(*) OVER w_student_id AS rows_in_student_id,
        ROUND((UNIX_TIMESTAMP(enrolled_at) - UNIX_TIMESTAMP(LAG(enrolled_at) OVER (ORDER BY enrolled_at_week DESC, id))) / 60, 1) AS minutes_since_prev_enrolled_at,
        ROUND(CUME_DIST() OVER (PARTITION BY grade_status, student_id ORDER BY enrolled_at_week ASC), 3) AS enrolled_at_week_cume_dist,
        ROUND(score - AVG(score) OVER (PARTITION BY student_id), 2) AS score_vs_student_id_avg
    FROM filtered
    WINDOW w_grade_status_student_id_by_e_2 AS (PARTITION BY grade_status, student_id ORDER BY enrolled_at_week ASC, id), w_student_id AS (PARTITION BY student_id), w_course_id_student_id AS (PARTITION BY course_id, student_id)
)
-- Final projection
SELECT
    id,
    student_id,
    course_id,
    score,
    grade_status,
    enrolled_at,
    enrolled_at_week,
    enrolled_at_week_dense_rank,
    prev2_enrolled_at,
    course_id_student_id_max_score,
    enrolled_at_week_percent_rank,
    min_score_so_far,
    course_id_student_id_sum_score,
    rows_in_student_id,
    minutes_since_prev_enrolled_at,
    enrolled_at_week_cume_dist,
    score_vs_student_id_avg
FROM windowed
WHERE score_vs_course_id_student_id_avg > -60.0
ORDER BY student_id
