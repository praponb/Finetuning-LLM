WITH scoped AS (
    -- Filter enrollments
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at + INTERVAL 1 DAYS AS enrolled_at_plus_1d,
        COALESCE(score, 0.0) AS score_filled
    FROM enrollments
    WHERE grade_status IN ('FAIL', 'INCOMPLETE', 'PASS')
      AND student_id IS NOT NULL
),
ranked AS (
    -- Previous / next values per key
    SELECT
        *,
        COUNT(*) OVER (PARTITION BY student_id, grade_status) AS rows_in_student_id_grade_status,
        ROUND(score / NULLIF(SUM(score) OVER w_student_id_grade_status, 0), 4) AS score_share_of_student_id_grade_status,
        MAX(score) OVER w_student_id_grade_status AS student_id_grade_status_max_score,
        MIN(score_filled) OVER w_student_id_grade_status AS student_id_grade_status_min_score_filled,
        ROUND(score_filled / NULLIF(SUM(score_filled) OVER w_student_id_term, 0), 4) AS score_filled_share_of_student_id_term,
        SIZE(COLLECT_SET(term) OVER w_student_id_grade_status) AS distinct_term_in_student_id_grade_status,
        ROUND(PERCENT_RANK() OVER w_all_by_score_filled_ties, 3) AS score_filled_percent_rank,
        ROUND(score - AVG(score) OVER (PARTITION BY student_id, term), 2) AS score_vs_student_id_term_avg,
        RANK() OVER (ORDER BY score) AS score_rank,
        SIZE(COLLECT_SET(term) OVER (PARTITION BY grade_status, student_id)) AS distinct_term_in_grade_status_student_id,
        MAX(score) OVER (ORDER BY score_filled DESC, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS score_max_7
    FROM scoped
    WINDOW w_student_id_grade_status AS (PARTITION BY student_id, grade_status), w_student_id_term AS (PARTITION BY student_id, term), w_all_by_score_filled_ties AS (ORDER BY score_filled DESC)
),
metrics AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROUND(score_filled_percent_rank / NULLIF(SUM(score_filled_percent_rank) OVER (PARTITION BY term), 0), 4) AS score_filled_percent_rank_share_of_term,
        ROW_NUMBER() OVER (ORDER BY score_filled_share_of_student_id_term NULLS LAST, id) AS rn,
        NTH_VALUE(score_max_7, 2) OVER (ORDER BY score_filled DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_score_max_7,
        MIN(score_max_7) OVER (ORDER BY score_filled DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_score_max_7_so_far,
        FIRST_VALUE(score_vs_student_id_term_avg) OVER (ORDER BY rows_in_student_id_grade_status DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_score_vs_student_id_term_avg
    FROM ranked
    WINDOW w_all_by_score_filled_share_of AS (ORDER BY score_filled_share_of_student_id_term NULLS LAST, id)
)
-- Final projection
SELECT
    id,
    student_id,
    course_id,
    term,
    score,
    grade_status,
    enrolled_at_plus_1d,
    score_filled,
    score_share_of_student_id_grade_status,
    student_id_grade_status_max_score,
    student_id_grade_status_min_score_filled,
    score_filled_share_of_student_id_term,
    distinct_term_in_student_id_grade_status,
    score_vs_student_id_term_avg,
    score_rank,
    distinct_term_in_grade_status_student_id,
    score_max_7,
    score_filled_percent_rank_share_of_term,
    second_score_max_7,
    min_score_max_7_so_far,
    first_score_vs_student_id_term_avg
FROM metrics
ORDER BY grade_status, id
LIMIT 5
