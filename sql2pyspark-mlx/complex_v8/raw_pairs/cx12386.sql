WITH filtered_enrollments AS (
    -- Filter enrollments
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status
    FROM enrollments
    WHERE term = '2024T2'
),
with_windows AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        course_id,
        term,
        score,
        grade_status,
        LAST_VALUE(score) OVER (PARTITION BY term, student_id ORDER BY score DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_score,
        MAX(score) OVER (PARTITION BY term, grade_status) AS term_grade_status_max_score,
        COUNT(*) OVER (PARTITION BY term, grade_status) AS rows_in_term_grade_status,
        NTILE(10) OVER (ORDER BY score, id) AS score_decile,
        SIZE(COLLECT_SET(student_id) OVER (PARTITION BY term, grade_status)) AS distinct_student_id_in_term_grade_status,
        ROUND(score / NULLIF(SUM(score) OVER (PARTITION BY term, grade_status), 0), 4) AS score_share_of_term_grade_status,
        LAG(score, 1, 0) OVER (PARTITION BY course_id ORDER BY score DESC, id) AS prev_score,
        MIN(score) OVER (PARTITION BY term, grade_status) AS term_grade_status_min_score
    FROM filtered_enrollments
),
tagged AS (
    -- Label and score rows
    SELECT *, FLOOR(prev_score / 25) * 25 AS prev_score_bucket
    FROM with_windows
    WHERE course_id IS NOT NULL
),
with_trends AS (
    -- Previous / next values per key
    SELECT
        id,
        term,
        grade_status,
        term_grade_status_max_score,
        rows_in_term_grade_status,
        score_decile,
        score_share_of_term_grade_status,
        prev_score,
        term_grade_status_min_score,
        RANK() OVER (PARTITION BY course_id, grade_status ORDER BY score_share_of_term_grade_status) AS score_share_of_term_grade_status_rank,
        LEAD(score_decile, 1, 0) OVER (PARTITION BY grade_status, distinct_student_id_in_term_grade_status ORDER BY term_grade_status_max_score, id) AS next_score_decile,
        DENSE_RANK() OVER w_course_id_grade_status_by_sc_2 AS score_share_of_term_grade_status_dense_rank
    FROM tagged
    WINDOW w_course_id_grade_status_by_sc_2 AS (PARTITION BY course_id, grade_status ORDER BY score_share_of_term_grade_status, id)
),
rollup_base AS (
    -- Summarise per group
    SELECT
        grade_status,
        PERCENTILE_APPROX(score_share_of_term_grade_status_rank, 0.5) AS median_score_share_of_term_grade_status_rank,
        COUNT(DISTINCT id) AS distinct_ids,
        ARRAY_SORT(COLLECT_SET(term)) AS term_list
    FROM with_trends
    GROUP BY grade_status
    HAVING SUM(term_grade_status_min_score) > 200.0
)
SELECT
    grade_status,
    median_score_share_of_term_grade_status_rank,
    distinct_ids,
    term_list,
    SUBSTRING(TRIM(grade_status), 1, 2) AS grade_status_prefix
FROM rollup_base
WHERE SIZE(term_list) >= 2
ORDER BY distinct_ids DESC NULLS FIRST, grade_status
LIMIT 5
