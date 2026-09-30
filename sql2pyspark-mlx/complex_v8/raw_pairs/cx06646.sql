WITH recent_enrollments AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        student_id,
        course_id,
        score,
        grade_status,
        enrolled_at
    FROM enrollments
    WHERE MONTH(enrolled_at) IN (1, 3)
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(score - AVG(score) OVER (PARTITION BY grade_status, DATE_TRUNC('MONTH', enrolled_at)), 2) AS score_vs_grade_status_enrolled_at_month_avg,
        LEAD(enrolled_at) OVER (PARTITION BY student_id, course_id ORDER BY score, id) AS next_enrolled_at,
        ROUND(PERCENT_RANK() OVER (PARTITION BY student_id ORDER BY score DESC), 3) AS score_percent_rank,
        FIRST_VALUE(enrolled_at) OVER (PARTITION BY student_id ORDER BY enrolled_at NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_enrolled_at,
        SIZE(COLLECT_SET(course_id) OVER (PARTITION BY grade_status, DATE_TRUNC('MONTH', enrolled_at))) AS distinct_course_id_in_grade_status_enrolled_at_m,
        ROUND(score - AVG(score) OVER (PARTITION BY student_id), 2) AS score_vs_student_id_avg,
        SUM(score) OVER (PARTITION BY student_id) AS student_id_sum_score,
        MAX(score) OVER (PARTITION BY student_id ORDER BY score DESC, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS score_max_3
    FROM recent_enrollments
),
labeled AS (
    -- Label and score rows
    SELECT
        *,
        DATEDIFF(DATE '2025-12-31', first_enrolled_at) AS days_since_first_enrolled_at,
        COALESCE(score_percent_rank, 0.0) AS score_percent_rank_filled
    FROM with_windows
),
summary AS (
    -- Aggregate with conditional metrics
    SELECT
        DATE_TRUNC('MONTH', first_enrolled_at) AS first_enrolled_at_month,
        distinct_course_id_in_grade_status_enrolled_at_m,
        DATE_TRUNC('MONTH', next_enrolled_at) AS next_enrolled_at_month,
        ROUND(AVG(score_max_3), 2) AS avg_score_max_3,
        SUM(CASE WHEN grade_status = 'INCOMPLETE' THEN 1 ELSE 0 END) AS n_grade_status_incomplete
    FROM labeled
    GROUP BY DATE_TRUNC('MONTH', first_enrolled_at), distinct_course_id_in_grade_status_enrolled_at_m, DATE_TRUNC('MONTH', next_enrolled_at)
)
SELECT
    first_enrolled_at_month,
    distinct_course_id_in_grade_status_enrolled_at_m,
    next_enrolled_at_month,
    avg_score_max_3,
    DATE_TRUNC('DAY', next_enrolled_at_month) AS next_enrolled_at_month_day
FROM summary
ORDER BY next_enrolled_at_month
