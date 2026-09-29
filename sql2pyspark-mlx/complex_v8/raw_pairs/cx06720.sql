WITH filtered_enrollments AS (
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
    WHERE grade_status = 'FAIL'
),
ranked AS (
    -- Previous / next values per key
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        DENSE_RANK() OVER (PARTITION BY term, course_id ORDER BY enrolled_at DESC) AS enrolled_at_dense_rank,
        MIN(score) OVER w_course_id_enrolled_at_month AS course_id_enrolled_at_month_min_score,
        MAX(score) OVER (PARTITION BY term) AS term_max_score,
        SIZE(COLLECT_SET(course_id) OVER w_term) AS distinct_course_id_in_term,
        SUM(score) OVER (PARTITION BY term) AS term_sum_score,
        MAX(score) OVER w_course_id_enrolled_at_month AS course_id_enrolled_at_month_max_score,
        MAX(score) OVER (ORDER BY score DESC, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS score_max_2,
        RANK() OVER (ORDER BY score ASC NULLS LAST, id) AS score_rank,
        ROUND(score - AVG(score) OVER w_term, 2) AS score_vs_term_avg,
        ROUND(score / NULLIF(SUM(score) OVER w_course_id_enrolled_at_month, 0), 4) AS score_share_of_course_id_enrolled_at_month,
        ROUND(score / NULLIF(SUM(score) OVER (PARTITION BY student_id), 0), 4) AS score_share_of_student_id,
        MAX(score) OVER (ORDER BY score DESC, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS score_max_4,
        SUM(score) OVER (PARTITION BY student_id) AS student_id_sum_score
    FROM filtered_enrollments
    WINDOW w_course_id_enrolled_at_month AS (PARTITION BY course_id, DATE_TRUNC('MONTH', enrolled_at)), w_term AS (PARTITION BY term)
),
enriched AS (
    -- Label and score rows
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        enrolled_at,
        enrolled_at_dense_rank,
        course_id_enrolled_at_month_min_score,
        term_max_score,
        distinct_course_id_in_term,
        score_max_2,
        score_rank,
        score_vs_term_avg,
        score_share_of_course_id_enrolled_at_month,
        score_share_of_student_id,
        score_max_4,
        MONTH(enrolled_at) AS enrolled_at_month
    FROM ranked
    WHERE student_id >= 17
),
sequenced AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        student_id,
        course_id,
        term,
        enrolled_at,
        enrolled_at_dense_rank,
        course_id_enrolled_at_month_min_score,
        term_max_score,
        score_max_2,
        score_vs_term_avg,
        score_share_of_student_id,
        NTH_VALUE(score_max_2, 2) OVER (ORDER BY enrolled_at_dense_rank DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_score_max_2,
        COUNT(*) OVER w_enrolled_at_month AS rows_in_enrolled_at_month,
        SUM(score) OVER w_enrolled_at_month AS enrolled_at_month_sum_score
    FROM enriched
    WINDOW w_enrolled_at_month AS (PARTITION BY enrolled_at_month)
),
per_group AS (
    -- Group-level totals
    SELECT
        student_id,
        enrolled_at_dense_rank,
        course_id,
        COUNT(*) FILTER (WHERE term NOT IN ('2024T2', '2024T1')) AS n_term_not_in_2024t2_2024t1,
        MAX(score_share_of_student_id) AS max_score_share_of_student_id
    FROM sequenced
    GROUP BY student_id, enrolled_at_dense_rank, course_id
    HAVING AVG(score_share_of_student_id) > 0.3
)
-- Final projection
SELECT
    student_id,
    enrolled_at_dense_rank,
    course_id,
    n_term_not_in_2024t2_2024t1,
    max_score_share_of_student_id
FROM per_group
WHERE course_id >= 5
ORDER BY student_id DESC, max_score_share_of_student_id NULLS LAST
