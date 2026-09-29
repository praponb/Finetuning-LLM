WITH prepared_enrollments AS (
    -- Scope the enrollments rows
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        enrolled_at,
        DATE_TRUNC('DAY', enrolled_at) AS enrolled_at_day
    FROM enrollments
    WHERE term <> '2025T1'
      AND enrolled_at >= TIMESTAMP '2025-01-13 11:00:00'
),
metrics AS (
    -- Previous / next values per key
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        enrolled_at,
        NTILE(5) OVER (ORDER BY score DESC, id) AS score_quintile,
        ROUND(score - AVG(score) OVER (PARTITION BY term, course_id), 2) AS score_vs_term_course_id_avg,
        ROUND(AVG(score) OVER (ORDER BY score DESC, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW), 2) AS score_avg_4,
        ROUND(CUME_DIST() OVER w_all_by_score, 3) AS score_cume_dist,
        LAG(score) OVER (ORDER BY score DESC, id) AS prev_score,
        MIN(score) OVER w_term_course_id AS term_course_id_min_score,
        DENSE_RANK() OVER (PARTITION BY term ORDER BY enrolled_at ASC) AS enrolled_at_dense_rank,
        NTILE(4) OVER (PARTITION BY term ORDER BY enrolled_at ASC, id) AS enrolled_at_quartile,
        LEAD(enrolled_at, 3) OVER (ORDER BY score DESC, id) AS next3_enrolled_at,
        COUNT(*) OVER (PARTITION BY term, course_id) AS rows_in_term_course_id,
        FIRST_VALUE(enrolled_at) OVER (ORDER BY score DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_enrolled_at,
        SUM(score) OVER (ORDER BY score DESC, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS score_sum_6_prior,
        SIZE(COLLECT_SET(student_id) OVER (PARTITION BY term, course_id)) AS distinct_student_id_in_term_course_id
    FROM prepared_enrollments
    WINDOW w_all_by_score AS (ORDER BY score DESC, id), w_term_course_id AS (PARTITION BY term, course_id)
),
ranked AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        term,
        score,
        enrolled_at,
        score_vs_term_course_id_avg,
        score_cume_dist,
        prev_score,
        rows_in_term_course_id,
        score_sum_6_prior,
        distinct_student_id_in_term_course_id,
        FIRST_VALUE(enrolled_at_quartile) OVER (PARTITION BY course_id ORDER BY first_enrolled_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_enrolled_at_quartile,
        COUNT(*) OVER (PARTITION BY enrolled_at_quartile, student_id) AS rows_in_enrolled_at_quartile_student_id,
        ROUND(CUME_DIST() OVER w_distinct_student_id_in_term, 3) AS rows_in_term_course_id_cume_dist,
        SUM(prev_score) OVER (PARTITION BY DATE_TRUNC('MONTH', next3_enrolled_at), enrolled_at_quartile ORDER BY enrolled_at, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS prev_score_sum_3
    FROM metrics
    WINDOW w_distinct_student_id_in_term AS (PARTITION BY distinct_student_id_in_term_course_id ORDER BY rows_in_term_course_id)
)
-- Final projection
SELECT
    id,
    term,
    score,
    score_vs_term_course_id_avg,
    rows_in_term_course_id,
    score_sum_6_prior,
    distinct_student_id_in_term_course_id,
    first_enrolled_at_quartile,
    rows_in_enrolled_at_quartile_student_id,
    prev_score_sum_3,
    ABS(prev_score - 32.9) AS prev_score_distance
FROM ranked
WHERE rows_in_enrolled_at_quartile_student_id >= 3
ORDER BY id DESC, prev_score_distance DESC
