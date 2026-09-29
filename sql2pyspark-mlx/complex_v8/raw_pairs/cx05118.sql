WITH valid AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        enrolled_at
    FROM enrollments
    WHERE term = '2025T2'
      AND grade_status = 'FAIL'
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(score / NULLIF(SUM(score) OVER w_term_course_id, 0), 4) AS score_share_of_term_course_id,
        ROUND(AVG(score) OVER (PARTITION BY term, course_id), 2) AS term_course_id_avg_score,
        COUNT(*) OVER (PARTITION BY course_id, term) AS rows_in_course_id_term,
        NTH_VALUE(enrolled_at, 2) OVER (ORDER BY enrolled_at ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_enrolled_at,
        DENSE_RANK() OVER w_student_id_by_score_ties AS score_dense_rank,
        ROUND(score - AVG(score) OVER w_term_course_id, 2) AS score_vs_term_course_id_avg,
        LEAD(enrolled_at, 3) OVER (ORDER BY enrolled_at ASC, id) AS next3_enrolled_at,
        RANK() OVER (PARTITION BY term, student_id ORDER BY score ASC NULLS LAST) AS score_rank,
        ROW_NUMBER() OVER (ORDER BY enrolled_at ASC, id) AS enrolled_at_seq
    FROM valid
    WINDOW w_term_course_id AS (PARTITION BY term, course_id), w_student_id_by_score_ties AS (PARTITION BY student_id ORDER BY score ASC)
),
leaders AS (
    -- First row per group
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        enrolled_at,
        score_share_of_term_course_id,
        term_course_id_avg_score,
        rows_in_course_id_term,
        second_enrolled_at,
        score_dense_rank,
        score_vs_term_course_id_avg,
        next3_enrolled_at,
        enrolled_at_seq
    FROM with_windows
    WHERE score_rank = 1
),
sequenced AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        score_share_of_term_course_id,
        score_dense_rank,
        score_vs_term_course_id_avg,
        next3_enrolled_at,
        MAX(score) OVER (PARTITION BY student_id ORDER BY enrolled_at DESC, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS score_max_3,
        LAST_VALUE(enrolled_at_seq) IGNORE NULLS OVER (PARTITION BY student_id, enrolled_at_seq ORDER BY enrolled_at_seq ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_enrolled_at_seq,
        MIN(score) OVER (PARTITION BY score_dense_rank) AS score_dense_rank_min_score,
        DENSE_RANK() OVER (PARTITION BY score_dense_rank ORDER BY score_dense_rank) AS score_dense_rank_dense_rank,
        SIZE(COLLECT_SET(term) OVER (PARTITION BY score_dense_rank)) AS distinct_term_in_score_dense_rank,
        ROUND(score_vs_term_course_id_avg - AVG(score_vs_term_course_id_avg) OVER (PARTITION BY score_dense_rank), 2) AS score_vs_term_course_id_avg_vs_score_dense_rank_
    FROM leaders
)
-- Final projection
SELECT
    id,
    student_id,
    term,
    score,
    score_dense_rank,
    next3_enrolled_at,
    score_max_3,
    last_known_enrolled_at_seq,
    score_dense_rank_dense_rank,
    distinct_term_in_score_dense_rank,
    score_vs_term_course_id_avg_vs_score_dense_rank_,
    COALESCE(score_dense_rank, 0) AS score_dense_rank_filled
FROM sequenced
ORDER BY distinct_term_in_score_dense_rank
