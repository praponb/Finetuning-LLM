WITH base_enrollments AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        CONCAT(term, '#', CAST(course_id AS STRING)) AS term_ref,
        LOWER(TRIM(term)) AS term_clean
    FROM enrollments
    WHERE student_id IS NOT NULL
      AND course_id IS NOT NULL
),
windowed AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        student_id,
        course_id,
        score,
        grade_status,
        term_clean,
        NTH_VALUE(enrolled_at, 2) OVER (PARTITION BY course_id, TO_DATE(enrolled_at) ORDER BY score, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_enrolled_at,
        score - LAG(score) OVER w_grade_status_student_id_by_e AS score_change,
        ROUND(score - AVG(score) OVER (PARTITION BY student_id, grade_status), 2) AS score_vs_student_id_grade_status_avg,
        ROUND(AVG(score) OVER (PARTITION BY student_id, grade_status), 2) AS student_id_grade_status_avg_score,
        ROUND(CUME_DIST() OVER (PARTITION BY student_id ORDER BY score, id), 3) AS score_cume_dist,
        MAX(score) OVER (PARTITION BY grade_status, student_id ORDER BY enrolled_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_score_so_far,
        ROUND(score / NULLIF(SUM(score) OVER w_student_id_grade_status, 0), 4) AS score_share_of_student_id_grade_status,
        RANK() OVER (PARTITION BY grade_status, student_id ORDER BY enrolled_at DESC) AS enrolled_at_rank,
        RANK() OVER (PARTITION BY course_id, TO_DATE(enrolled_at) ORDER BY score, id) AS score_rank,
        LEAD(enrolled_at, 2) OVER (PARTITION BY student_id ORDER BY score, id) AS next2_enrolled_at
    FROM base_enrollments
    WINDOW w_grade_status_student_id_by_e AS (PARTITION BY grade_status, student_id ORDER BY enrolled_at DESC, id), w_student_id_grade_status AS (PARTITION BY student_id, grade_status)
),
top_rows AS (
    -- First row per group
    SELECT *
    FROM windowed
    WHERE score_rank = 1
      AND grade_status IS NOT NULL
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        student_id,
        course_id,
        score,
        grade_status,
        term_clean,
        second_enrolled_at,
        score_vs_student_id_grade_status_avg,
        student_id_grade_status_avg_score,
        score_cume_dist,
        max_score_so_far,
        score_share_of_student_id_grade_status,
        enrolled_at_rank,
        score_rank,
        ROUND(score - AVG(score) OVER (PARTITION BY score_rank, student_id), 2) AS score_vs_score_rank_student_id_avg,
        ROW_NUMBER() OVER w_all_by_max_score_so_far AS seq_no,
        ROUND(score_change - AVG(score_change) OVER (PARTITION BY DATE_TRUNC('MONTH', second_enrolled_at), grade_status), 2) AS score_change_vs_second_enrolled_at_month_grade_s,
        ROUND(PERCENT_RANK() OVER (ORDER BY next2_enrolled_at DESC, id), 3) AS next2_enrolled_at_percent_rank,
        ROUND((UNIX_TIMESTAMP(second_enrolled_at) - UNIX_TIMESTAMP(LAG(second_enrolled_at) OVER (PARTITION BY course_id, score_rank ORDER BY next2_enrolled_at DESC, id))) / 60, 1) AS minutes_since_prev_second_enrolled_at
    FROM top_rows
    WINDOW w_all_by_max_score_so_far AS (ORDER BY max_score_so_far DESC, id)
)
-- Final projection
SELECT
    id,
    student_id,
    score,
    grade_status,
    term_clean,
    second_enrolled_at,
    student_id_grade_status_avg_score,
    score_cume_dist,
    max_score_so_far,
    score_share_of_student_id_grade_status,
    enrolled_at_rank,
    score_rank,
    score_vs_score_rank_student_id_avg,
    seq_no,
    FLOOR(enrolled_at_rank / 2) * 2 AS enrolled_at_rank_bucket
FROM with_windows
WHERE course_id <= 9
ORDER BY enrolled_at_rank DESC
