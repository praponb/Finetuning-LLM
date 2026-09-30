WITH scoped_enrollments AS (
    -- Filter enrollments
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        CASE WHEN term IN ('2025T1', '2024T1') THEN 'group_a' WHEN term = '2025T2' THEN 'group_b' ELSE 'other' END AS term_group,
        LENGTH(TRIM(term)) AS term_len
    FROM enrollments
),
with_trends AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(AVG(score) OVER (PARTITION BY term_group, term_len ORDER BY enrolled_at DESC NULLS FIRST, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING), 2) AS score_avg_2_prior,
        COUNT(*) OVER w_term_student_id AS rows_in_term_student_id,
        SIZE(COLLECT_SET(course_id) OVER w_term_student_id) AS distinct_course_id_in_term_student_id,
        LAST_VALUE(term_len) OVER (PARTITION BY term_group, term_len ORDER BY enrolled_at DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_term_len
    FROM scoped_enrollments
    WINDOW w_term_student_id AS (PARTITION BY term, student_id)
),
per_group AS (
    -- Group-level totals
    SELECT
        DATE_TRUNC('MONTH', enrolled_at) AS enrolled_at_month,
        COUNT(*) FILTER (WHERE grade_status IS NOT NULL) AS n_grade_status_is_not_null,
        ROUND(SUM(score) / NULLIF(SUM(distinct_course_id_in_term_student_id), 0), 3) AS score_to_distinct_course_id_in_term_student_id_r,
        SUM(distinct_course_id_in_term_student_id) AS total_distinct_course_id_in_term_student_id
    FROM with_trends
    GROUP BY DATE_TRUNC('MONTH', enrolled_at)
    HAVING AVG(score) > 8.7
),
metrics AS (
    -- Previous / next values per key
    SELECT
        *,
        NTILE(10) OVER (ORDER BY total_distinct_course_id_in_term_student_id DESC, enrolled_at_month) AS total_distinct_course_id_in_term_student_id_deci,
        RANK() OVER w_all_by_total_distinct_course_2 AS total_distinct_course_id_in_term_student_id_rank
    FROM per_group
    WINDOW w_all_by_total_distinct_course_2 AS (ORDER BY total_distinct_course_id_in_term_student_id ASC)
),
with_trends_2 AS (
    -- Window metrics over several partitions
    SELECT
        enrolled_at_month,
        n_grade_status_is_not_null,
        score_to_distinct_course_id_in_term_student_id_r,
        total_distinct_course_id_in_term_student_id,
        total_distinct_course_id_in_term_student_id_deci,
        ROUND(n_grade_status_is_not_null / NULLIF(SUM(n_grade_status_is_not_null) OVER (PARTITION BY total_distinct_course_id_in_term_student_id_rank, n_grade_status_is_not_null), 0), 4) AS n_grade_status_is_not_null_share_of_total_distin,
        ROUND(CUME_DIST() OVER w_all_by_score_to_distinct_cou, 3) AS score_to_distinct_course_id_in_term_student_id_r_2,
        ROUND(CUME_DIST() OVER (PARTITION BY n_grade_status_is_not_null ORDER BY n_grade_status_is_not_null), 3) AS n_grade_status_is_not_null_cume_dist,
        COUNT(*) OVER w_total_distinct_course_id_in AS rows_in_total_distinct_course_id_in_term_student
    FROM metrics
    WINDOW w_all_by_score_to_distinct_cou AS (ORDER BY score_to_distinct_course_id_in_term_student_id_r), w_total_distinct_course_id_in AS (PARTITION BY total_distinct_course_id_in_term_student_id_rank, n_grade_status_is_not_null)
)
-- Subtotals with ROLLUP
SELECT
    n_grade_status_is_not_null,
    total_distinct_course_id_in_term_student_id_deci,
    GROUPING(n_grade_status_is_not_null) AS is_total_n_grade_status_is_not_null,
    MIN(enrolled_at_month) AS first_enrolled_at_month,
    ROUND(AVG(score_to_distinct_course_id_in_term_student_id_r), 2) AS avg_score_to_distinct_course_id_in_term_student_
FROM with_trends_2
GROUP BY ROLLUP(n_grade_status_is_not_null, total_distinct_course_id_in_term_student_id_deci)
ORDER BY n_grade_status_is_not_null NULLS LAST, total_distinct_course_id_in_term_student_id_deci NULLS LAST
