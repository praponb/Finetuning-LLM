WITH base AS (
    -- Filter enrollments
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status
    FROM enrollments
    WHERE (course_id <= 5 OR enrolled_at < TIMESTAMP '2025-02-24 08:00:00')
),
ranked AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        LAG(score) OVER (PARTITION BY term, grade_status ORDER BY score DESC, id) AS prev_score,
        ROUND(score / NULLIF(SUM(score) OVER (PARTITION BY student_id), 0), 4) AS score_share_of_student_id,
        SIZE(COLLECT_SET(term) OVER w_student_id) AS distinct_term_in_student_id,
        score - LAG(score) OVER (PARTITION BY course_id ORDER BY score ASC, id) AS score_change,
        LAG(score, 2) OVER w_student_id_by_score AS prev2_score,
        MAX(score) OVER (PARTITION BY term, grade_status ORDER BY score DESC, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS score_max_7,
        NTH_VALUE(score, 2) OVER (PARTITION BY course_id ORDER BY score ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_score,
        COUNT(*) OVER w_student_id AS rows_in_student_id,
        MIN(score) OVER w_student_id AS student_id_min_score,
        ROUND(CUME_DIST() OVER (PARTITION BY student_id ORDER BY score ASC), 3) AS score_cume_dist,
        ROUND(AVG(score) OVER (PARTITION BY term, grade_status ORDER BY score DESC, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW), 2) AS score_avg_2,
        RANK() OVER (PARTITION BY student_id ORDER BY score ASC, id) AS score_rank,
        LAG(score, 3) OVER (PARTITION BY course_id ORDER BY score ASC, id) AS prev3_score,
        ROUND(AVG(score) OVER w_student_id, 2) AS student_id_avg_score,
        SUM(score) OVER (PARTITION BY student_id ORDER BY score ASC, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS score_sum_2
    FROM base
    WINDOW w_student_id AS (PARTITION BY student_id), w_student_id_by_score AS (PARTITION BY student_id ORDER BY score ASC, id)
),
first_rows AS (
    -- Top 5 per group
    SELECT *
    FROM ranked
    WHERE score_rank <= 5
),
with_windows AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        student_id,
        course_id,
        grade_status,
        prev_score,
        score_share_of_student_id,
        score_change,
        score_max_7,
        second_score,
        rows_in_student_id,
        student_id_min_score,
        score_cume_dist,
        score_avg_2,
        prev3_score,
        student_id_avg_score,
        score_sum_2,
        distinct_term_in_student_id - LAG(distinct_term_in_student_id) OVER w_all_by_prev3_score AS distinct_term_in_student_id_change,
        COUNT(*) OVER (PARTITION BY score_rank) AS rows_in_score_rank
    FROM first_rows
    WINDOW w_all_by_prev3_score AS (ORDER BY prev3_score, id)
)
SELECT
    id,
    student_id,
    course_id,
    grade_status,
    score_share_of_student_id,
    score_change,
    second_score,
    rows_in_student_id,
    student_id_min_score,
    score_cume_dist,
    prev3_score,
    student_id_avg_score,
    score_sum_2,
    distinct_term_in_student_id_change,
    rows_in_score_rank,
    CONCAT(grade_status, '#', CAST(rows_in_score_rank AS STRING)) AS grade_status_ref
FROM with_windows
ORDER BY score_change DESC
