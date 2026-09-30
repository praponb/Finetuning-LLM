WITH prepared AS (
    -- Filter enrollments
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at
    FROM enrollments
    WHERE grade_status <> 'INCOMPLETE'
      AND student_id >= 13
),
sequenced AS (
    -- Window metrics over several partitions
    SELECT
        id,
        course_id,
        term,
        grade_status,
        enrolled_at,
        ROUND(AVG(score) OVER (PARTITION BY grade_status, TO_DATE(enrolled_at) ORDER BY enrolled_at DESC NULLS FIRST, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW), 2) AS score_avg_2,
        DENSE_RANK() OVER (ORDER BY score DESC) AS score_dense_rank,
        ROUND(AVG(score) OVER (PARTITION BY grade_status, term), 2) AS grade_status_term_avg_score,
        ROW_NUMBER() OVER (ORDER BY enrolled_at, id) AS rn,
        ROUND(AVG(score) OVER (PARTITION BY student_id, DATE_TRUNC('MONTH', enrolled_at)), 2) AS student_id_enrolled_at_month_avg_score,
        SUM(score) OVER w_course_id AS course_id_sum_score,
        LAG(score) OVER (ORDER BY enrolled_at ASC, id) AS prev_score,
        RANK() OVER w_student_id_grade_status_by_s AS score_rank,
        SUM(score) OVER (PARTITION BY student_id, DATE_TRUNC('MONTH', enrolled_at)) AS student_id_enrolled_at_month_sum_score,
        SUM(score) OVER (PARTITION BY grade_status, term) AS grade_status_term_sum_score,
        MAX(score) OVER (PARTITION BY course_id) AS course_id_max_score,
        COUNT(*) OVER (PARTITION BY student_id, DATE_TRUNC('MONTH', enrolled_at)) AS rows_in_student_id_enrolled_at_month,
        FIRST_VALUE(enrolled_at) OVER (ORDER BY enrolled_at ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_enrolled_at
    FROM prepared
    WINDOW w_course_id AS (PARTITION BY course_id), w_student_id_grade_status_by_s AS (PARTITION BY student_id, grade_status ORDER BY score DESC, id)
),
stats AS (
    -- Aggregate with conditional metrics
    SELECT
        rn,
        term,
        COUNT(*) AS cnt,
        ARRAY_SORT(COLLECT_SET(term)) AS term_list,
        PERCENTILE_APPROX(score_rank, 0.5) AS median_score_rank,
        MIN(rows_in_student_id_enrolled_at_month) AS min_rows_in_student_id_enrolled_at_month,
        ROUND(AVG(grade_status_term_sum_score), 2) AS avg_grade_status_term_sum_score
    FROM sequenced
    GROUP BY rn, term
),
metrics AS (
    -- Previous / next values per key
    SELECT
        *,
        ROW_NUMBER() OVER w_cnt_by_median_score_rank AS rn_2,
        FIRST_VALUE(cnt) OVER (PARTITION BY cnt ORDER BY median_score_rank DESC, rn, term ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_cnt
    FROM stats
    WINDOW w_cnt_by_median_score_rank AS (PARTITION BY cnt ORDER BY median_score_rank DESC, rn, term)
)
-- Final projection
SELECT DISTINCT term, cnt
FROM metrics
ORDER BY term
