WITH scoped AS (
    -- Filter enrollments
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        enrolled_at
    FROM enrollments
    WHERE student_id IS NOT NULL
      AND term = '2025T1'
),
ranked AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        term,
        score,
        enrolled_at,
        MIN(score) OVER (PARTITION BY term, course_id) AS term_course_id_min_score,
        NTILE(3) OVER (PARTITION BY student_id, course_id ORDER BY score, id) AS score_tercile,
        ROUND(score - AVG(score) OVER w_student_id_term, 2) AS score_vs_student_id_term_avg,
        LAST_VALUE(score) OVER (PARTITION BY student_id, course_id ORDER BY score, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_score,
        RANK() OVER (PARTITION BY student_id, course_id ORDER BY score) AS score_rank,
        ROUND(score / NULLIF(SUM(score) OVER w_term_course_id, 0), 4) AS score_share_of_term_course_id,
        LEAD(enrolled_at) OVER (PARTITION BY course_id, term ORDER BY enrolled_at DESC, id) AS next_enrolled_at,
        NTH_VALUE(score, 2) OVER (ORDER BY enrolled_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_score,
        LAST_VALUE(enrolled_at) IGNORE NULLS OVER (PARTITION BY course_id, term ORDER BY enrolled_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_enrolled_at,
        LEAD(enrolled_at, 3) OVER w_student_id_by_enrolled_at AS next3_enrolled_at,
        LEAD(enrolled_at) OVER (ORDER BY enrolled_at DESC, id) AS next_enrolled_at_2
    FROM scoped
    WINDOW w_student_id_term AS (PARTITION BY student_id, term), w_term_course_id AS (PARTITION BY term, course_id), w_student_id_by_enrolled_at AS (PARTITION BY student_id ORDER BY enrolled_at, id)
),
first_rows AS (
    -- Top 5 per group
    SELECT
        id,
        term,
        score,
        enrolled_at,
        term_course_id_min_score,
        score_tercile,
        score_vs_student_id_term_avg,
        last_score,
        score_share_of_term_course_id,
        next_enrolled_at,
        second_score,
        last_known_enrolled_at,
        next3_enrolled_at,
        next_enrolled_at_2
    FROM ranked
    WHERE score_rank <= 5
      AND TO_DATE(next_enrolled_at) < DATE '2025-02-12'
),
ranked_2 AS (
    -- Window metrics over several partitions
    SELECT
        id,
        term,
        term_course_id_min_score,
        score_vs_student_id_term_avg,
        score_share_of_term_course_id,
        next_enrolled_at,
        last_known_enrolled_at,
        next3_enrolled_at,
        next_enrolled_at_2,
        MAX(score_vs_student_id_term_avg) OVER w_enrolled_at_month AS enrolled_at_month_max_score_vs_student_id_term_a,
        LAG(score, 1, 0) OVER (PARTITION BY term ORDER BY next_enrolled_at_2, id) AS prev_score,
        NTILE(10) OVER (PARTITION BY term ORDER BY next_enrolled_at_2, id) AS next_enrolled_at_2_decile,
        FIRST_VALUE(enrolled_at) IGNORE NULLS OVER (PARTITION BY term, MONTH(enrolled_at) ORDER BY term_course_id_min_score DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_enrolled_at,
        SUM(last_score) OVER (PARTITION BY MONTH(enrolled_at)) AS enrolled_at_month_sum_last_score,
        ROUND(CUME_DIST() OVER w_term_enrolled_at_month_by_te_2, 3) AS term_course_id_min_score_cume_dist
    FROM first_rows
    WINDOW w_enrolled_at_month AS (PARTITION BY MONTH(enrolled_at)), w_term_enrolled_at_month_by_te_2 AS (PARTITION BY term, MONTH(enrolled_at) ORDER BY term_course_id_min_score DESC)
)
SELECT
    id,
    term,
    term_course_id_min_score,
    score_vs_student_id_term_avg,
    last_known_enrolled_at,
    next3_enrolled_at,
    next_enrolled_at_2,
    enrolled_at_month_max_score_vs_student_id_term_a,
    prev_score,
    first_known_enrolled_at,
    term_course_id_min_score_cume_dist,
    FLOOR(prev_score / 25) * 25 AS prev_score_bucket,
    LENGTH(TRIM(term)) AS term_len
FROM ranked_2
ORDER BY prev_score_bucket DESC, term_course_id_min_score_cume_dist NULLS LAST
