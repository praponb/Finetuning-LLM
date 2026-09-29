WITH filtered_enrollments AS (
    -- Scope the enrollments rows
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status
    FROM enrollments
),
enriched_course AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.term,
        s.grade_status,
        c.credits AS course_credits
    FROM filtered_enrollments s
    JOIN courses c ON c.id = s.course_id
),
metrics AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        student_id,
        course_id,
        term,
        MAX(course_credits) OVER (PARTITION BY student_id, term ORDER BY course_credits DESC, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS course_credits_max_3_prior,
        ROUND(AVG(course_credits) OVER (PARTITION BY course_credits, student_id), 2) AS course_credits_student_id_avg_course_credits,
        ROUND(course_credits - AVG(course_credits) OVER (PARTITION BY course_credits, student_id), 2) AS course_credits_vs_course_credits_student_id_avg,
        RANK() OVER w_student_id_term_by_course_cr_2 AS course_credits_rank,
        SIZE(COLLECT_SET(course_id) OVER w_course_credits_student_id) AS distinct_course_id_in_course_credits_student_id,
        LAST_VALUE(course_credits) OVER (PARTITION BY student_id, term ORDER BY course_credits DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_course_credits,
        ROUND(AVG(course_credits) OVER (PARTITION BY course_id ORDER BY course_credits DESC, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS course_credits_avg_7,
        ROW_NUMBER() OVER (PARTITION BY student_id, term ORDER BY course_credits DESC, id) AS course_credits_seq
    FROM enriched_course
    WINDOW w_student_id_term_by_course_cr_2 AS (PARTITION BY student_id, term ORDER BY course_credits DESC), w_course_credits_student_id AS (PARTITION BY course_credits, student_id)
),
scored AS (
    -- Add calculated columns
    SELECT
        id,
        student_id,
        course_id,
        term,
        course_credits_max_3_prior,
        course_credits_vs_course_credits_student_id_avg,
        course_credits_rank,
        last_course_credits,
        course_credits_avg_7,
        course_credits_seq,
        SUBSTRING(TRIM(term), 1, 2) AS term_prefix,
        ROUND(course_credits_avg_7 * 1.1, 2) AS course_credits_avg_7_adj
    FROM metrics
),
with_windows AS (
    -- Window metrics over several partitions
    SELECT
        id,
        student_id,
        course_id,
        course_credits_max_3_prior,
        last_course_credits,
        course_credits_avg_7,
        course_credits_seq,
        term_prefix,
        course_credits_avg_7_adj,
        ROUND(PERCENT_RANK() OVER (PARTITION BY course_credits_max_3_prior, course_id ORDER BY course_credits_rank DESC), 3) AS course_credits_rank_percent_rank,
        ROUND(course_credits_avg_7_adj - AVG(course_credits_avg_7_adj) OVER w_course_id_term, 2) AS course_credits_avg_7_adj_vs_course_id_term_avg,
        ROW_NUMBER() OVER (PARTITION BY course_id, student_id ORDER BY last_course_credits ASC NULLS LAST, id) AS seq_no,
        MAX(course_credits_max_3_prior) OVER (PARTITION BY last_course_credits, course_credits_seq) AS last_course_credits_course_credits_seq_max_cours
    FROM scored
    WINDOW w_course_id_term AS (PARTITION BY course_id, term)
),
summary AS (
    -- Group-level totals
    SELECT
        course_credits_seq,
        CASE WHEN course_credits_avg_7_adj >= 5.0 THEN 'A' WHEN course_credits_avg_7_adj >= 2.8 THEN 'B' ELSE 'C' END AS course_credits_avg_7_adj_tier,
        last_course_credits,
        ROUND(STDDEV(last_course_credits_course_credits_seq_max_cours), 2) AS stddev_last_course_credits_course_credits_seq_ma,
        COUNT(*) AS n_rows,
        COUNT(DISTINCT id) AS distinct_ids,
        ROUND(STDDEV(last_course_credits), 2) AS stddev_last_course_credits
    FROM with_windows
    GROUP BY course_credits_seq, CASE WHEN course_credits_avg_7_adj >= 5.0 THEN 'A' WHEN course_credits_avg_7_adj >= 2.8 THEN 'B' ELSE 'C' END, last_course_credits
),
with_trends AS (
    -- Running and moving aggregates
    SELECT
        *,
        NTILE(10) OVER w_all_by_stddev_last_course_cr AS stddev_last_course_credits_course_credits_seq_ma_2,
        RANK() OVER w_all_by_stddev_last_course_cr_2 AS stddev_last_course_credits_course_credits_seq_ma_3,
        ROUND(AVG(stddev_last_course_credits) OVER w_distinct_ids, 2) AS distinct_ids_avg_stddev_last_course_credits
    FROM summary
    WINDOW w_all_by_stddev_last_course_cr AS (ORDER BY stddev_last_course_credits_course_credits_seq_ma DESC, course_credits_seq, course_credits_avg_7_adj_tier, last_course_credits), w_all_by_stddev_last_course_cr_2 AS (ORDER BY stddev_last_course_credits_course_credits_seq_ma DESC), w_distinct_ids AS (PARTITION BY distinct_ids)
)
SELECT
    course_credits_seq,
    course_credits_avg_7_adj_tier,
    stddev_last_course_credits_course_credits_seq_ma,
    distinct_ids,
    stddev_last_course_credits,
    stddev_last_course_credits_course_credits_seq_ma_2,
    stddev_last_course_credits_course_credits_seq_ma_3,
    distinct_ids_avg_stddev_last_course_credits,
    n_rows + last_course_credits AS n_rows_plus_last_course_credits
FROM with_trends
ORDER BY stddev_last_course_credits DESC
