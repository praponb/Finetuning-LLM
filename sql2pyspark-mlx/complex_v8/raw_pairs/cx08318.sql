WITH filtered AS (
    -- Filter enrollments
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        enrolled_at,
        LOWER(TRIM(grade_status)) AS grade_status_clean
    FROM enrollments
    WHERE course_id <= 9
      AND grade_status = 'FAIL'
),
sequenced AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        student_id,
        course_id,
        score,
        grade_status_clean,
        ROW_NUMBER() OVER (PARTITION BY term ORDER BY enrolled_at, id) AS enrolled_at_seq,
        RANK() OVER (ORDER BY enrolled_at DESC) AS enrolled_at_rank,
        MAX(score) OVER (PARTITION BY term) AS term_max_score,
        LAST_VALUE(enrolled_at) IGNORE NULLS OVER (PARTITION BY course_id ORDER BY enrolled_at DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_enrolled_at,
        SUM(score) OVER (ORDER BY enrolled_at DESC, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS score_sum_4,
        LEAD(score) OVER (PARTITION BY course_id ORDER BY enrolled_at DESC NULLS FIRST, id) AS next_score,
        COUNT(*) OVER (PARTITION BY term) AS rows_in_term,
        SUM(score) OVER (ORDER BY enrolled_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_score,
        FIRST_VALUE(score) OVER (ORDER BY enrolled_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_score,
        RANK() OVER (PARTITION BY course_id ORDER BY enrolled_at DESC NULLS FIRST, id) AS enrolled_at_rank_2,
        LAST_VALUE(enrolled_at) OVER (PARTITION BY course_id ORDER BY enrolled_at DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_enrolled_at
    FROM filtered
),
top_rows AS (
    -- Top 5 per group
    SELECT
        id,
        student_id,
        course_id,
        score,
        grade_status_clean,
        enrolled_at_seq,
        term_max_score,
        last_known_enrolled_at,
        score_sum_4,
        next_score,
        rows_in_term,
        running_score,
        first_score,
        enrolled_at_rank_2,
        last_enrolled_at
    FROM sequenced
    WHERE enrolled_at_rank <= 5
),
windowed AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        NTILE(4) OVER (PARTITION BY student_id ORDER BY first_score, id) AS first_score_quartile,
        DENSE_RANK() OVER (PARTITION BY student_id ORDER BY next_score DESC) AS next_score_dense_rank,
        LAST_VALUE(next_score) OVER (PARTITION BY student_id ORDER BY first_score, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_next_score,
        COUNT(*) OVER (PARTITION BY enrolled_at_seq) AS rows_in_enrolled_at_seq,
        ROUND(AVG(next_score) OVER (PARTITION BY enrolled_at_seq), 2) AS enrolled_at_seq_avg_next_score
    FROM top_rows
),
summary AS (
    -- Aggregate with conditional metrics
    SELECT
        DATE_TRUNC('MONTH', last_enrolled_at) AS last_enrolled_at_month,
        ROUND(STDDEV(enrolled_at_rank_2), 2) AS stddev_enrolled_at_rank_2,
        COUNT(DISTINCT course_id) AS distinct_course_ids,
        ROUND(SUM(term_max_score) / NULLIF(SUM(enrolled_at_rank_2), 0), 3) AS term_max_score_to_enrolled_at_rank_2_ratio,
        SUM(last_next_score) AS total_last_next_score,
        ROUND(AVG(enrolled_at_rank_2), 2) AS avg_enrolled_at_rank_2,
        PERCENTILE_APPROX(rows_in_term, 0.5) AS median_rows_in_term
    FROM windowed
    WHERE rows_in_enrolled_at_seq BETWEEN 4 AND 18
    GROUP BY 1
),
ranked AS (
    -- Window metrics over several partitions
    SELECT
        *,
        DENSE_RANK() OVER (PARTITION BY distinct_course_ids ORDER BY avg_enrolled_at_rank_2 ASC NULLS LAST) AS avg_enrolled_at_rank_2_dense_rank,
        ROUND(median_rows_in_term - AVG(median_rows_in_term) OVER w_distinct_course_ids_last_enr, 2) AS median_rows_in_term_vs_distinct_course_ids_last_,
        LAG(total_last_next_score, 2) OVER (PARTITION BY distinct_course_ids ORDER BY avg_enrolled_at_rank_2 ASC NULLS LAST, last_enrolled_at_month) AS prev2_total_last_next_score,
        ROUND(AVG(total_last_next_score) OVER w_distinct_course_ids_last_enr, 2) AS distinct_course_ids_last_enrolled_at_month_year_
    FROM summary
    WINDOW w_distinct_course_ids_last_enr AS (PARTITION BY distinct_course_ids, YEAR(last_enrolled_at_month))
)
SELECT
    last_enrolled_at_month,
    stddev_enrolled_at_rank_2,
    distinct_course_ids,
    term_max_score_to_enrolled_at_rank_2_ratio,
    total_last_next_score,
    avg_enrolled_at_rank_2,
    median_rows_in_term,
    avg_enrolled_at_rank_2_dense_rank,
    median_rows_in_term_vs_distinct_course_ids_last_,
    prev2_total_last_next_score,
    distinct_course_ids_last_enrolled_at_month_year_,
    DATE_FORMAT(last_enrolled_at_month, 'MMM yyyy') AS last_enrolled_at_month_label,
    COALESCE(median_rows_in_term, 0) AS median_rows_in_term_filled
FROM ranked
WHERE term_max_score_to_enrolled_at_rank_2_ratio > 3.5
ORDER BY avg_enrolled_at_rank_2 NULLS LAST, stddev_enrolled_at_rank_2, last_enrolled_at_month
LIMIT 5
