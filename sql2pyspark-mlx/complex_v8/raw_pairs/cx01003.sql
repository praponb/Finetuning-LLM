WITH base AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        student_id,
        course_id,
        score,
        grade_status,
        enrolled_at
    FROM enrollments
    WHERE (enrolled_at >= TIMESTAMP '2025-01-19 07:00:00' OR grade_status = 'PASS')
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        course_id,
        score,
        grade_status,
        enrolled_at,
        SUM(score) OVER (PARTITION BY course_id ORDER BY enrolled_at, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS score_sum_4,
        ROW_NUMBER() OVER w_enrolled_at_month_student_id AS seq_no,
        SIZE(COLLECT_SET(student_id) OVER (PARTITION BY grade_status, course_id)) AS distinct_student_id_in_grade_status_course_id,
        ROUND(score - AVG(score) OVER w_grade_status_course_id, 2) AS score_vs_grade_status_course_id_avg,
        ROUND(score - AVG(score) OVER (PARTITION BY course_id), 2) AS score_vs_course_id_avg,
        MIN(score) OVER (PARTITION BY TO_DATE(enrolled_at) ORDER BY enrolled_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_score_so_far,
        NTILE(4) OVER w_student_id_grade_status_by_s AS score_quartile,
        LEAD(score) OVER w_course_id_student_id_by_enro AS next_score,
        NTILE(5) OVER (PARTITION BY DATE_TRUNC('MONTH', enrolled_at), student_id ORDER BY score ASC, id) AS score_quintile,
        ROUND(AVG(score) OVER (PARTITION BY TO_DATE(enrolled_at) ORDER BY enrolled_at, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS score_avg_7
    FROM base
    WINDOW w_enrolled_at_month_student_id AS (PARTITION BY DATE_TRUNC('MONTH', enrolled_at), student_id ORDER BY score ASC, id), w_grade_status_course_id AS (PARTITION BY grade_status, course_id), w_student_id_grade_status_by_s AS (PARTITION BY student_id, grade_status ORDER BY score, id), w_course_id_student_id_by_enro AS (PARTITION BY course_id, student_id ORDER BY enrolled_at DESC NULLS FIRST, id)
),
with_trends AS (
    -- Previous / next values per key
    SELECT
        *,
        MAX(score_avg_7) OVER w_distinct_student_id_in_grade AS distinct_student_id_in_grade_status_course_id_ma,
        COUNT(*) OVER w_distinct_student_id_in_grade AS rows_in_distinct_student_id_in_grade_status_cour,
        LAST_VALUE(score_quartile) OVER (ORDER BY score_quartile, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_score_quartile,
        ROW_NUMBER() OVER (ORDER BY score_vs_course_id_avg DESC, id) AS rn,
        NTILE(4) OVER (PARTITION BY course_id ORDER BY score_quintile DESC NULLS FIRST, id) AS score_quintile_quartile,
        SUM(score_quartile) OVER (ORDER BY score_vs_course_id_avg DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_score_quartile,
        ROUND(CUME_DIST() OVER (ORDER BY score_quartile), 3) AS score_quartile_cume_dist,
        NTH_VALUE(enrolled_at, 2) OVER (ORDER BY score_vs_course_id_avg DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_enrolled_at
    FROM with_windows
    WINDOW w_distinct_student_id_in_grade AS (PARTITION BY distinct_student_id_in_grade_status_course_id)
)
SELECT
    id,
    course_id,
    grade_status,
    enrolled_at,
    score_sum_4,
    seq_no,
    distinct_student_id_in_grade_status_course_id,
    score_vs_grade_status_course_id_avg,
    score_vs_course_id_avg,
    min_score_so_far,
    score_quartile,
    next_score,
    distinct_student_id_in_grade_status_course_id_ma,
    rows_in_distinct_student_id_in_grade_status_cour,
    last_score_quartile,
    rn,
    score_quartile_cume_dist,
    second_enrolled_at
FROM with_trends
