WITH prepared_enrollments AS (
    -- Scope the enrollments rows
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status
    FROM enrollments
    WHERE student_id IS NOT NULL
),
windowed AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        NTILE(5) OVER (PARTITION BY term, course_id ORDER BY score, id) AS score_quintile,
        LEAD(score) OVER w_all_by_score AS next_score,
        ROUND(score - AVG(score) OVER (PARTITION BY term, course_id), 2) AS score_vs_term_course_id_avg,
        ROUND(score / NULLIF(SUM(score) OVER w_term_course_id, 0), 4) AS score_share_of_term_course_id,
        DENSE_RANK() OVER w_term_course_id_by_score_ties AS score_dense_rank,
        ROUND(AVG(score) OVER (PARTITION BY course_id), 2) AS course_id_avg_score,
        COUNT(*) OVER (PARTITION BY term, course_id ORDER BY score, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count,
        ROUND(AVG(score) OVER (PARTITION BY term, course_id), 2) AS term_course_id_avg_score,
        ROUND(AVG(score) OVER (PARTITION BY grade_status), 2) AS grade_status_avg_score,
        ROUND(PERCENT_RANK() OVER (PARTITION BY student_id ORDER BY score DESC, id), 3) AS score_percent_rank,
        LEAD(score) OVER (PARTITION BY grade_status ORDER BY score DESC, id) AS next_score_2,
        LAST_VALUE(score) IGNORE NULLS OVER (PARTITION BY grade_status ORDER BY score, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_score,
        ROUND(score - AVG(score) OVER (PARTITION BY course_id), 2) AS score_vs_course_id_avg
    FROM prepared_enrollments
    WINDOW w_all_by_score AS (ORDER BY score ASC, id), w_term_course_id AS (PARTITION BY term, course_id), w_term_course_id_by_score_ties AS (PARTITION BY term, course_id ORDER BY score)
),
derived AS (
    -- Add calculated columns
    SELECT
        id,
        student_id,
        course_id,
        score_quintile,
        next_score,
        score_share_of_term_course_id,
        term_course_id_avg_score,
        grade_status_avg_score,
        score_percent_rank,
        next_score_2,
        score_vs_course_id_avg,
        score_quintile + next_score AS score_quintile_plus_next_score,
        ROUND(score_quintile / NULLIF(running_count, 0), 3) AS score_quintile_per_running_count
    FROM windowed
)
SELECT
    id,
    student_id,
    course_id,
    score_quintile,
    next_score,
    score_share_of_term_course_id,
    grade_status_avg_score,
    score_percent_rank,
    next_score_2,
    score_quintile_plus_next_score,
    ROUND(next_score_2 * 1.1, 2) AS next_score_2_adj,
    COALESCE(term_course_id_avg_score, 0.0) AS term_course_id_avg_score_filled
FROM derived
ORDER BY term_course_id_avg_score_filled NULLS LAST
