WITH base_enrollments AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        SUBSTRING(TRIM(term), 1, 2) AS term_prefix
    FROM enrollments
    WHERE score < 80.2
),
sequenced AS (
    -- Running and moving aggregates
    SELECT
        id,
        course_id,
        term,
        score,
        term_prefix,
        NTILE(3) OVER (PARTITION BY grade_status, term ORDER BY score DESC, id) AS score_tercile,
        score - LAG(score) OVER (ORDER BY score DESC, id) AS score_change,
        ROUND(AVG(score) OVER (PARTITION BY grade_status, term ORDER BY score DESC, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING), 2) AS score_avg_3_prior,
        ROUND(score / NULLIF(SUM(score) OVER (PARTITION BY student_id), 0), 4) AS score_share_of_student_id,
        DENSE_RANK() OVER (PARTITION BY grade_status, term ORDER BY score DESC) AS score_dense_rank,
        FIRST_VALUE(score) OVER (ORDER BY score DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_score,
        RANK() OVER (ORDER BY score ASC) AS score_rank,
        ROUND(AVG(score) OVER (ORDER BY score DESC, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING), 2) AS score_avg_2_prior,
        ROUND(score / NULLIF(SUM(score) OVER (PARTITION BY student_id, grade_status), 0), 4) AS score_share_of_student_id_grade_status
    FROM base_enrollments
),
top_per_group AS (
    -- First row per group
    SELECT *
    FROM sequenced
    WHERE score_dense_rank = 1
      AND score_change >= -10.0
),
windowed AS (
    -- Previous / next values per key
    SELECT
        *,
        MAX(score_rank) OVER (PARTITION BY score_dense_rank, score_rank ORDER BY score_avg_2_prior DESC, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS score_rank_max_6_prior,
        SIZE(COLLECT_SET(course_id) OVER w_score_tercile) AS distinct_course_id_in_score_tercile,
        COUNT(*) OVER (PARTITION BY score_tercile) AS rows_in_score_tercile,
        LEAD(score_rank, 3) OVER (PARTITION BY score_dense_rank, score_rank ORDER BY score_avg_2_prior DESC, id) AS next3_score_rank,
        MIN(first_score) OVER (PARTITION BY term, score_tercile ORDER BY score_rank, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_first_score_so_far,
        SIZE(COLLECT_SET(term) OVER (PARTITION BY score_tercile)) AS distinct_term_in_score_tercile
    FROM top_per_group
    WINDOW w_score_tercile AS (PARTITION BY score_tercile)
)
SELECT
    id,
    term,
    score,
    term_prefix,
    score_tercile,
    score_change,
    score_avg_3_prior,
    score_share_of_student_id,
    score_dense_rank,
    first_score,
    score_rank,
    score_avg_2_prior,
    score_share_of_student_id_grade_status,
    score_rank_max_6_prior,
    distinct_course_id_in_score_tercile,
    rows_in_score_tercile,
    next3_score_rank,
    distinct_term_in_score_tercile,
    LOWER(REGEXP_REPLACE(TRIM(term), ' ', '_')) AS term_slug,
    LOWER(REGEXP_REPLACE(TRIM(term), ' ', '_')) AS term_slug_2
FROM windowed
WHERE score_tercile BETWEEN 4 AND 7
ORDER BY score_change NULLS LAST, distinct_course_id_in_score_tercile DESC
