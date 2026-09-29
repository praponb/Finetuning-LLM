WITH filtered AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        student_id,
        course_id,
        term,
        grade_status,
        enrolled_at
    FROM enrollments
    WHERE student_id <= 3
      AND term IN ('2024T1', '2025T2')
),
windowed AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(CUME_DIST() OVER (ORDER BY enrolled_at), 3) AS enrolled_at_cume_dist,
        NTILE(10) OVER w_all_by_enrolled_at AS enrolled_at_decile,
        DENSE_RANK() OVER (ORDER BY enrolled_at) AS enrolled_at_dense_rank,
        DENSE_RANK() OVER w_term_by_enrolled_at_ties AS enrolled_at_dense_rank_2,
        LEAD(enrolled_at) OVER (ORDER BY enrolled_at NULLS LAST, id) AS next_enrolled_at,
        LAST_VALUE(enrolled_at) OVER (PARTITION BY course_id ORDER BY enrolled_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_enrolled_at,
        ROUND((UNIX_TIMESTAMP(enrolled_at) - UNIX_TIMESTAMP(LAG(enrolled_at) OVER w_all_by_enrolled_at)) / 60, 1) AS minutes_since_prev_enrolled_at,
        ROUND(PERCENT_RANK() OVER (PARTITION BY course_id ORDER BY enrolled_at, id), 3) AS enrolled_at_percent_rank,
        DENSE_RANK() OVER (ORDER BY enrolled_at, id) AS enrolled_at_dense_rank_3,
        NTILE(4) OVER (PARTITION BY term ORDER BY enrolled_at, id) AS enrolled_at_quartile,
        FIRST_VALUE(enrolled_at) OVER (ORDER BY enrolled_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_enrolled_at,
        LAG(enrolled_at) OVER (PARTITION BY term ORDER BY enrolled_at, id) AS prev_enrolled_at,
        LAG(enrolled_at, 2) OVER (ORDER BY enrolled_at, id) AS prev2_enrolled_at
    FROM filtered
    WINDOW w_all_by_enrolled_at AS (ORDER BY enrolled_at NULLS LAST, id), w_term_by_enrolled_at_ties AS (PARTITION BY term ORDER BY enrolled_at)
),
leaders AS (
    -- First row per group
    SELECT *
    FROM windowed
    WHERE enrolled_at_dense_rank_3 = 1
      AND enrolled_at_percent_rank > 0.5
)
SELECT
    id,
    student_id,
    grade_status,
    enrolled_at,
    enrolled_at_cume_dist,
    enrolled_at_decile,
    enrolled_at_dense_rank_2,
    next_enrolled_at,
    last_enrolled_at,
    minutes_since_prev_enrolled_at,
    enrolled_at_percent_rank,
    enrolled_at_dense_rank_3,
    prev_enrolled_at,
    ABS(enrolled_at_dense_rank_2 - 6) AS enrolled_at_dense_rank_2_distance,
    DATE_FORMAT(enrolled_at, 'yyyy-MM-dd HH:00') AS enrolled_at_label
FROM leaders
ORDER BY enrolled_at_label DESC, id
LIMIT 5
