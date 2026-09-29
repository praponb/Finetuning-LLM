WITH prepared AS (
    -- Scope the enrollments rows
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at
    FROM enrollments
    WHERE (score >= 5.9 OR term IS NOT NULL)
),
windowed AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROUND(score - AVG(score) OVER (PARTITION BY term), 2) AS score_vs_term_avg,
        RANK() OVER w_grade_status_by_enrolled_at AS enrolled_at_rank,
        ROUND(score - AVG(score) OVER (PARTITION BY grade_status, course_id), 2) AS score_vs_grade_status_course_id_avg,
        FIRST_VALUE(score) OVER (ORDER BY score ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_score,
        NTH_VALUE(score, 2) OVER (PARTITION BY term, grade_status ORDER BY score DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_score,
        COUNT(*) OVER (PARTITION BY term, student_id ORDER BY score, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count,
        MAX(score) OVER (PARTITION BY grade_status ORDER BY enrolled_at DESC, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS score_max_1_prior,
        ROW_NUMBER() OVER w_term_grade_status_by_score AS rn,
        SUM(score) OVER (ORDER BY score ASC, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS score_sum_3,
        MAX(score) OVER (PARTITION BY term) AS term_max_score,
        ROW_NUMBER() OVER w_term_student_id_by_score AS rn_2,
        ROUND(AVG(score) OVER (ORDER BY score, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW), 2) AS score_avg_2
    FROM prepared
    WINDOW w_grade_status_by_enrolled_at AS (PARTITION BY grade_status ORDER BY enrolled_at DESC), w_term_grade_status_by_score AS (PARTITION BY term, grade_status ORDER BY score DESC, id), w_term_student_id_by_score AS (PARTITION BY term, student_id ORDER BY score, id)
),
top_per_group AS (
    -- Top 2 per group
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        score_vs_term_avg,
        score_vs_grade_status_course_id_avg,
        first_score,
        second_score,
        running_count,
        score_max_1_prior,
        rn,
        score_sum_3,
        term_max_score,
        rn_2,
        score_avg_2
    FROM windowed
    WHERE enrolled_at_rank <= 2
      AND score_vs_grade_status_course_id_avg >= -80.0
),
with_windows AS (
    -- Window metrics over several partitions
    SELECT
        id,
        course_id,
        term,
        grade_status,
        score_vs_term_avg,
        first_score,
        second_score,
        score_max_1_prior,
        term_max_score,
        rn_2,
        score_avg_2,
        ROUND(first_score - AVG(first_score) OVER (PARTITION BY rn_2), 2) AS first_score_vs_rn_2_avg,
        COUNT(*) OVER (PARTITION BY rn_2) AS rows_in_rn_2,
        LAG(score_vs_term_avg, 3) OVER (ORDER BY second_score ASC, id) AS prev3_score_vs_term_avg,
        RANK() OVER w_course_id_by_second_score AS second_score_rank
    FROM top_per_group
    WINDOW w_course_id_by_second_score AS (PARTITION BY course_id ORDER BY second_score ASC, id)
)
-- Final projection
SELECT
    id,
    course_id,
    term,
    grade_status,
    first_score,
    second_score,
    score_max_1_prior,
    term_max_score,
    rn_2,
    score_avg_2,
    first_score_vs_rn_2_avg,
    rows_in_rn_2,
    prev3_score_vs_term_avg,
    second_score_rank,
    ROUND(score_max_1_prior * 0.9, 2) AS score_max_1_prior_adj
FROM with_windows
