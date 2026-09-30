WITH cleaned AS (
    -- Filter enrollments
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        enrolled_at,
        enrolled_at + INTERVAL 1 DAYS AS enrolled_at_plus_1d
    FROM enrollments
    WHERE course_id >= 4
      AND enrolled_at >= TIMESTAMP '2025-01-19 11:00:00'
),
with_trends AS (
    -- Previous / next values per key
    SELECT
        *,
        COUNT(*) OVER (PARTITION BY MONTH(enrolled_at_plus_1d)) AS rows_in_enrolled_at_plus_1d_month,
        ROUND(PERCENT_RANK() OVER (PARTITION BY student_id, course_id ORDER BY enrolled_at, id), 3) AS enrolled_at_percent_rank,
        SUM(score) OVER (PARTITION BY YEAR(enrolled_at_plus_1d)) AS enrolled_at_plus_1d_year_sum_score,
        LAG(score) OVER (PARTITION BY student_id, course_id ORDER BY enrolled_at, id) AS prev_score,
        SUM(score) OVER (PARTITION BY student_id, course_id ORDER BY enrolled_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_score,
        RANK() OVER (PARTITION BY course_id ORDER BY enrolled_at_plus_1d) AS enrolled_at_plus_1d_rank,
        SUM(score) OVER (PARTITION BY YEAR(enrolled_at_plus_1d), course_id ORDER BY enrolled_at DESC, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS score_sum_7,
        DENSE_RANK() OVER (PARTITION BY YEAR(enrolled_at_plus_1d), course_id ORDER BY enrolled_at DESC, id) AS enrolled_at_dense_rank
    FROM cleaned
),
first_rows AS (
    -- Top 2 per group
    SELECT *
    FROM with_trends
    WHERE enrolled_at_dense_rank <= 2
),
ranked AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROUND(running_score / NULLIF(SUM(running_score) OVER (PARTITION BY term), 0), 4) AS running_score_share_of_term,
        running_score - LAG(running_score) OVER (PARTITION BY enrolled_at_plus_1d_rank, term ORDER BY enrolled_at_percent_rank, id) AS running_score_change,
        NTILE(4) OVER w_enrolled_at_plus_1d_rank_ter AS enrolled_at_percent_rank_quartile,
        COUNT(*) OVER (PARTITION BY term) AS rows_in_term,
        ROUND(AVG(enrolled_at_plus_1d_rank) OVER (PARTITION BY enrolled_at_plus_1d_rank, term ORDER BY enrolled_at_percent_rank, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS enrolled_at_plus_1d_rank_avg_7,
        ROW_NUMBER() OVER (PARTITION BY course_id ORDER BY score_sum_7 DESC NULLS FIRST, id) AS score_sum_7_seq,
        MAX(enrolled_at_plus_1d_rank) OVER (PARTITION BY term) AS term_max_enrolled_at_plus_1d_rank
    FROM first_rows
    WINDOW w_enrolled_at_plus_1d_rank_ter AS (PARTITION BY enrolled_at_plus_1d_rank, term ORDER BY enrolled_at_percent_rank, id)
),
totals AS (
    -- Summarise per group
    SELECT
        DATE_TRUNC('MONTH', enrolled_at) AS enrolled_at_month,
        enrolled_at_percent_rank_quartile,
        student_id,
        ARRAY_SORT(COLLECT_SET(term)) AS term_list,
        SUM(enrolled_at_percent_rank_quartile) FILTER (WHERE term = '2024T2') AS enrolled_at_percent_rank_quartile_term_2024t2,
        ROUND(STDDEV(enrolled_at_percent_rank_quartile), 2) AS stddev_enrolled_at_percent_rank_quartile
    FROM ranked
    WHERE enrolled_at_dense_rank >= 3
    GROUP BY DATE_TRUNC('MONTH', enrolled_at), enrolled_at_percent_rank_quartile, student_id
    HAVING AVG(enrolled_at_plus_1d_year_sum_score) > 350.0
)
SELECT
    enrolled_at_month,
    enrolled_at_percent_rank_quartile,
    student_id,
    term_list,
    stddev_enrolled_at_percent_rank_quartile,
    DATE_TRUNC('QUARTER', enrolled_at_month) AS enrolled_at_month_quarter,
    enrolled_at_month + INTERVAL 1 DAYS AS enrolled_at_month_plus_1d
FROM totals
ORDER BY enrolled_at_month_quarter, student_id DESC, enrolled_at_month, enrolled_at_percent_rank_quartile
LIMIT 5
