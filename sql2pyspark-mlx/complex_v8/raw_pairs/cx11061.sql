WITH recent_enrollments AS (
    -- Filter enrollments
    SELECT
        id,
        student_id,
        course_id,
        term,
        grade_status,
        enrolled_at,
        COALESCE(score, 0.0) AS score_filled
    FROM enrollments
    WHERE score <= 77.0
      AND grade_status = 'INCOMPLETE'
),
with_windows AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROUND(score_filled - AVG(score_filled) OVER w_student_id_term, 2) AS score_filled_vs_student_id_term_avg,
        FIRST_VALUE(score_filled) OVER (ORDER BY enrolled_at ASC NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_score_filled,
        SUM(score_filled) OVER (PARTITION BY DATE_TRUNC('MONTH', enrolled_at), course_id) AS enrolled_at_month_course_id_sum_score_filled,
        ROUND(PERCENT_RANK() OVER (PARTITION BY course_id ORDER BY score_filled ASC), 3) AS score_filled_percent_rank,
        SUM(score_filled) OVER (ORDER BY score_filled DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_score_filled,
        NTH_VALUE(enrolled_at, 2) OVER (PARTITION BY term, student_id ORDER BY score_filled DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_enrolled_at,
        DENSE_RANK() OVER (ORDER BY score_filled DESC NULLS FIRST, id) AS score_filled_dense_rank,
        SIZE(COLLECT_SET(grade_status) OVER w_student_id_term) AS distinct_grade_status_in_student_id_term,
        DENSE_RANK() OVER w_student_id_course_id_by_scor AS score_filled_dense_rank_2,
        ROUND(PERCENT_RANK() OVER (PARTITION BY student_id, course_id ORDER BY score_filled DESC), 3) AS score_filled_percent_rank_2,
        ROUND(AVG(score_filled) OVER (PARTITION BY term, student_id ORDER BY score_filled DESC, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW), 2) AS score_filled_avg_2,
        ROUND(score_filled / NULLIF(SUM(score_filled) OVER (PARTITION BY student_id, term), 0), 4) AS score_filled_share_of_student_id_term,
        ROUND(PERCENT_RANK() OVER w_course_id_by_score_filled, 3) AS score_filled_percent_rank_3
    FROM recent_enrollments
    WINDOW w_student_id_term AS (PARTITION BY student_id, term), w_student_id_course_id_by_scor AS (PARTITION BY student_id, course_id ORDER BY score_filled DESC), w_course_id_by_score_filled AS (PARTITION BY course_id ORDER BY score_filled ASC, id)
),
scored AS (
    -- Label and score rows
    SELECT
        id,
        student_id,
        course_id,
        term,
        score_filled,
        score_filled_vs_student_id_term_avg,
        first_score_filled,
        enrolled_at_month_course_id_sum_score_filled,
        score_filled_percent_rank,
        running_score_filled,
        second_enrolled_at,
        score_filled_dense_rank,
        distinct_grade_status_in_student_id_term,
        score_filled_dense_rank_2,
        score_filled_percent_rank_2,
        score_filled_avg_2,
        score_filled_percent_rank_3,
        FLOOR(first_score_filled / 25) * 25 AS first_score_filled_bucket,
        COALESCE(score_filled_dense_rank, 0) AS score_filled_dense_rank_filled
    FROM with_windows
    WHERE score_filled_share_of_student_id_term < 0.7
),
with_trends AS (
    -- Running and moving aggregates
    SELECT
        *,
        NTILE(4) OVER (PARTITION BY score_filled_dense_rank ORDER BY running_score_filled ASC, id) AS running_score_filled_quartile,
        ROUND(AVG(score_filled_avg_2) OVER (PARTITION BY term, student_id), 2) AS term_student_id_avg_score_filled_avg_2,
        SUM(first_score_filled_bucket) OVER (PARTITION BY score_filled_dense_rank_filled, distinct_grade_status_in_student_id_term ORDER BY score_filled_dense_rank DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_first_score_filled_bucket,
        score_filled_percent_rank_2 - LAG(score_filled_percent_rank_2) OVER (ORDER BY enrolled_at_month_course_id_sum_score_filled, id) AS score_filled_percent_rank_2_change,
        score_filled_percent_rank - LAG(score_filled_percent_rank) OVER (PARTITION BY score_filled_dense_rank ORDER BY running_score_filled ASC, id) AS score_filled_percent_rank_change,
        LEAD(score_filled, 2, 0) OVER (PARTITION BY course_id ORDER BY enrolled_at_month_course_id_sum_score_filled ASC, id) AS next2_score_filled
    FROM scored
)
-- Final projection
SELECT DISTINCT score_filled_dense_rank_2, score_filled_percent_rank_2, id
FROM with_trends
ORDER BY score_filled_percent_rank_2, score_filled_dense_rank_2
