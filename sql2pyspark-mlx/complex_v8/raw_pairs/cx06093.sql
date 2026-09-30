WITH valid_enrollments AS (
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
    WHERE score < 85.2
      AND course_id IS NOT NULL
),
windowed AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(score - AVG(score) OVER (PARTITION BY YEAR(enrolled_at)), 2) AS score_vs_enrolled_at_year_avg,
        DENSE_RANK() OVER (PARTITION BY student_id, grade_status ORDER BY enrolled_at DESC) AS enrolled_at_dense_rank,
        FIRST_VALUE(enrolled_at) IGNORE NULLS OVER (PARTITION BY course_id, grade_status ORDER BY score DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_enrolled_at,
        ROUND(CUME_DIST() OVER (ORDER BY enrolled_at DESC), 3) AS enrolled_at_cume_dist,
        NTILE(5) OVER w_student_id_grade_status_by_e_2 AS enrolled_at_quintile,
        ROUND(score - AVG(score) OVER w_student_id, 2) AS score_vs_student_id_avg,
        ROUND(score / NULLIF(SUM(score) OVER (PARTITION BY YEAR(enrolled_at)), 0), 4) AS score_share_of_enrolled_at_year,
        COUNT(*) OVER (PARTITION BY YEAR(enrolled_at)) AS rows_in_enrolled_at_year,
        ROUND(score / NULLIF(SUM(score) OVER w_student_id, 0), 4) AS score_share_of_student_id,
        RANK() OVER (PARTITION BY student_id, grade_status ORDER BY enrolled_at DESC, id) AS enrolled_at_rank,
        LEAD(score, 2, 0) OVER (PARTITION BY course_id, grade_status ORDER BY score DESC, id) AS next2_score,
        RANK() OVER (PARTITION BY student_id, grade_status ORDER BY enrolled_at DESC) AS enrolled_at_rank_2,
        RANK() OVER w_course_id_grade_status_by_sc_2 AS score_rank
    FROM valid_enrollments
    WINDOW w_student_id_grade_status_by_e_2 AS (PARTITION BY student_id, grade_status ORDER BY enrolled_at DESC, id), w_student_id AS (PARTITION BY student_id), w_course_id_grade_status_by_sc_2 AS (PARTITION BY course_id, grade_status ORDER BY score DESC, id)
),
top_rows AS (
    -- Top 2 per group
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        score_vs_enrolled_at_year_avg,
        enrolled_at_dense_rank,
        first_known_enrolled_at,
        enrolled_at_cume_dist,
        enrolled_at_quintile,
        score_vs_student_id_avg,
        score_share_of_enrolled_at_year,
        rows_in_enrolled_at_year,
        score_share_of_student_id,
        enrolled_at_rank,
        next2_score,
        enrolled_at_rank_2
    FROM windowed
    WHERE score_rank <= 2
)
-- Final projection
SELECT
    id,
    student_id,
    course_id,
    grade_status,
    enrolled_at,
    score_vs_enrolled_at_year_avg,
    enrolled_at_dense_rank,
    first_known_enrolled_at,
    enrolled_at_cume_dist,
    enrolled_at_quintile,
    score_vs_student_id_avg,
    score_share_of_enrolled_at_year,
    rows_in_enrolled_at_year,
    score_share_of_student_id,
    enrolled_at_rank,
    next2_score,
    DATE_TRUNC('QUARTER', first_known_enrolled_at) AS first_known_enrolled_at_quarter
FROM top_rows
ORDER BY rows_in_enrolled_at_year DESC, score_vs_enrolled_at_year_avg
