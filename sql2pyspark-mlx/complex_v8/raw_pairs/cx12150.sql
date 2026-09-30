WITH recent_enrollments AS (
    -- Scope the enrollments rows
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        enrolled_at,
        LENGTH(TRIM(term)) AS term_len
    FROM enrollments
    WHERE term <> '2025T2'
      AND MONTH(enrolled_at) IN (1, 3)
),
ranked AS (
    -- Running and moving aggregates
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        enrolled_at,
        term_len,
        NTH_VALUE(score, 2) OVER (PARTITION BY course_id ORDER BY term_len DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_score,
        MIN(term_len) OVER w_term AS term_min_term_len,
        NTILE(10) OVER w_all_by_term_len AS term_len_decile,
        ROW_NUMBER() OVER w_all_by_score AS seq_no,
        DENSE_RANK() OVER (PARTITION BY course_id ORDER BY term_len DESC, id) AS term_len_dense_rank,
        LAG(term_len) OVER (ORDER BY score ASC, id) AS prev_term_len,
        NTILE(4) OVER (PARTITION BY course_id ORDER BY term_len DESC, id) AS term_len_quartile,
        COUNT(*) OVER (PARTITION BY term) AS rows_in_term,
        LEAD(enrolled_at) OVER (ORDER BY score ASC, id) AS next_enrolled_at,
        SIZE(COLLECT_SET(student_id) OVER w_term) AS distinct_student_id_in_term,
        ROUND(score - AVG(score) OVER (PARTITION BY term), 2) AS score_vs_term_avg,
        RANK() OVER w_all_by_term_len_ties AS term_len_rank,
        ROUND(term_len - AVG(term_len) OVER (PARTITION BY term), 2) AS term_len_vs_term_avg,
        SIZE(COLLECT_SET(course_id) OVER (PARTITION BY term)) AS distinct_course_id_in_term,
        ROUND(AVG(term_len) OVER (ORDER BY term_len, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING), 2) AS term_len_avg_6_prior
    FROM recent_enrollments
    WINDOW w_term AS (PARTITION BY term), w_all_by_term_len AS (ORDER BY term_len, id), w_all_by_score AS (ORDER BY score ASC, id), w_all_by_term_len_ties AS (ORDER BY term_len)
),
leaders AS (
    -- Top 5 per group
    SELECT *
    FROM ranked
    WHERE seq_no <= 5
),
sequenced AS (
    -- Window metrics over several partitions
    SELECT
        id,
        student_id,
        course_id,
        score,
        enrolled_at,
        term_len,
        second_score,
        term_min_term_len,
        seq_no,
        prev_term_len,
        term_len_quartile,
        next_enrolled_at,
        distinct_student_id_in_term,
        score_vs_term_avg,
        term_len_rank,
        term_len_vs_term_avg,
        distinct_course_id_in_term,
        RANK() OVER w_prev_term_len_by_prev_term_l AS prev_term_len_rank,
        ROW_NUMBER() OVER (PARTITION BY prev_term_len ORDER BY prev_term_len NULLS LAST, id) AS seq_no_2,
        SUM(score_vs_term_avg) OVER (PARTITION BY prev_term_len ORDER BY prev_term_len NULLS LAST, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS score_vs_term_avg_sum_2_prior,
        ROUND(AVG(term_len_quartile) OVER (PARTITION BY prev_term_len ORDER BY prev_term_len NULLS LAST, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW), 2) AS term_len_quartile_avg_4
    FROM leaders
    WINDOW w_prev_term_len_by_prev_term_l AS (PARTITION BY prev_term_len ORDER BY prev_term_len NULLS LAST, id)
)
SELECT
    id,
    student_id,
    score,
    term_len,
    term_min_term_len,
    seq_no,
    prev_term_len,
    term_len_quartile,
    next_enrolled_at,
    distinct_student_id_in_term,
    score_vs_term_avg,
    term_len_rank,
    term_len_vs_term_avg,
    distinct_course_id_in_term,
    seq_no_2,
    score_vs_term_avg_sum_2_prior,
    DATEDIFF(DATE '2026-01-01', enrolled_at) AS days_since_enrolled_at,
    term_len_quartile + term_len_vs_term_avg AS term_len_quartile_plus_term_len_vs_term_avg
FROM sequenced
ORDER BY term_len_quartile_plus_term_len_vs_term_avg DESC, id
LIMIT 20
