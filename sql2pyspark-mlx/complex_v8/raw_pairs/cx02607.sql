WITH filtered_enrollments AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        enrolled_at + INTERVAL 2 HOURS AS enrolled_at_plus_2h,
        MONTH(enrolled_at) AS enrolled_at_month
    FROM enrollments
    WHERE score <= 76.8
      AND grade_status NOT IN ('FAIL', 'WITHDRAWN')
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        SIZE(COLLECT_SET(course_id) OVER (PARTITION BY student_id)) AS distinct_course_id_in_student_id,
        LAST_VALUE(enrolled_at_plus_2h) OVER (ORDER BY enrolled_at_month, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_enrolled_at_plus_2h,
        MAX(enrolled_at_month) OVER (PARTITION BY student_id) AS student_id_max_enrolled_at_month,
        ROW_NUMBER() OVER (PARTITION BY grade_status, student_id ORDER BY enrolled_at_plus_2h DESC, id) AS enrolled_at_plus_2h_seq,
        NTH_VALUE(enrolled_at_plus_2h, 2) OVER (PARTITION BY grade_status, student_id ORDER BY enrolled_at_plus_2h DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_enrolled_at_plus_2h,
        ROUND(score / NULLIF(SUM(score) OVER w_student_id, 0), 4) AS score_share_of_student_id,
        MIN(score) OVER (ORDER BY enrolled_at_month, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_score_so_far,
        LEAD(enrolled_at) OVER (ORDER BY enrolled_at_month, id) AS next_enrolled_at,
        FIRST_VALUE(score) IGNORE NULLS OVER (ORDER BY enrolled_at_month, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_score,
        DENSE_RANK() OVER (PARTITION BY grade_status, student_id ORDER BY enrolled_at_plus_2h DESC) AS enrolled_at_plus_2h_dense_rank,
        ROUND(AVG(score) OVER (PARTITION BY student_id), 2) AS student_id_avg_score,
        SIZE(COLLECT_SET(term) OVER (PARTITION BY student_id)) AS distinct_term_in_student_id,
        RANK() OVER w_all_by_enrolled_at_month_tie AS enrolled_at_month_rank,
        SUM(score) OVER (ORDER BY enrolled_at_month DESC, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS score_sum_2,
        ROUND(enrolled_at_month / NULLIF(SUM(enrolled_at_month) OVER (PARTITION BY student_id), 0), 4) AS enrolled_at_month_share_of_student_id,
        SIZE(COLLECT_SET(grade_status) OVER (PARTITION BY student_id)) AS distinct_grade_status_in_student_id
    FROM filtered_enrollments
    WINDOW w_student_id AS (PARTITION BY student_id), w_all_by_enrolled_at_month_tie AS (ORDER BY enrolled_at_month)
),
top_rows AS (
    -- Top 5 per group
    SELECT *
    FROM metrics
    WHERE enrolled_at_plus_2h_seq <= 5
      AND enrolled_at_month > 3
),
ranked AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        student_id,
        course_id,
        term,
        grade_status,
        enrolled_at,
        enrolled_at_plus_2h,
        enrolled_at_month,
        distinct_course_id_in_student_id,
        last_enrolled_at_plus_2h,
        student_id_max_enrolled_at_month,
        enrolled_at_plus_2h_seq,
        second_enrolled_at_plus_2h,
        score_share_of_student_id,
        min_score_so_far,
        next_enrolled_at,
        first_known_score,
        enrolled_at_plus_2h_dense_rank,
        student_id_avg_score,
        distinct_term_in_student_id,
        enrolled_at_month_rank,
        enrolled_at_month_share_of_student_id,
        distinct_grade_status_in_student_id,
        LEAD(distinct_term_in_student_id) OVER (PARTITION BY enrolled_at_month ORDER BY enrolled_at_plus_2h_seq DESC, id) AS next_distinct_term_in_student_id,
        RANK() OVER (PARTITION BY DATE_TRUNC('MONTH', last_enrolled_at_plus_2h) ORDER BY second_enrolled_at_plus_2h DESC NULLS FIRST) AS second_enrolled_at_plus_2h_rank,
        distinct_term_in_student_id - LAG(distinct_term_in_student_id) OVER (PARTITION BY DATE_TRUNC('MONTH', last_enrolled_at_plus_2h) ORDER BY second_enrolled_at_plus_2h DESC NULLS FIRST, id) AS distinct_term_in_student_id_change,
        RANK() OVER w_course_id_enrolled_at_plus_2 AS enrolled_at_plus_2h_seq_rank
    FROM top_rows
    WINDOW w_course_id_enrolled_at_plus_2 AS (PARTITION BY course_id, TO_DATE(enrolled_at_plus_2h) ORDER BY enrolled_at_plus_2h_seq DESC, id)
),
stats AS (
    -- Summarise per group
    SELECT
        DATE_TRUNC('MONTH', last_enrolled_at_plus_2h) AS last_enrolled_at_plus_2h_month,
        enrolled_at_plus_2h_seq_rank,
        term,
        COUNT(DISTINCT term) AS distinct_terms,
        MAX(second_enrolled_at_plus_2h) AS last_second_enrolled_at_plus_2h,
        COUNT(*) AS row_count,
        COUNT(DISTINCT course_id) AS distinct_course_ids
    FROM ranked
    GROUP BY 1, 2, 3
)
-- Final projection
SELECT
    last_enrolled_at_plus_2h_month,
    enrolled_at_plus_2h_seq_rank,
    term,
    distinct_terms,
    last_second_enrolled_at_plus_2h,
    distinct_course_ids,
    LOWER(REGEXP_REPLACE(TRIM(term), ' ', '_')) AS term_slug
FROM stats
ORDER BY distinct_course_ids DESC, last_second_enrolled_at_plus_2h
