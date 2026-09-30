WITH base_enrollments AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        student_id,
        course_id,
        term,
        grade_status,
        enrolled_at,
        LOWER(REGEXP_REPLACE(TRIM(term), ' ', '_')) AS term_slug
    FROM enrollments
    WHERE HOUR(enrolled_at) BETWEEN 8 AND 14
),
metrics AS (
    -- Previous / next values per key
    SELECT
        id,
        term,
        enrolled_at,
        term_slug,
        LAG(enrolled_at, 2) OVER w_all_by_enrolled_at AS prev2_enrolled_at,
        NTH_VALUE(enrolled_at, 2) OVER (PARTITION BY course_id, term ORDER BY enrolled_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_enrolled_at,
        ROUND((UNIX_TIMESTAMP(enrolled_at) - UNIX_TIMESTAMP(LAG(enrolled_at) OVER (ORDER BY enrolled_at DESC, id))) / 60, 1) AS minutes_since_prev_enrolled_at,
        LAST_VALUE(enrolled_at) IGNORE NULLS OVER (ORDER BY enrolled_at NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_enrolled_at,
        DENSE_RANK() OVER (ORDER BY enrolled_at NULLS LAST, id) AS enrolled_at_dense_rank,
        FIRST_VALUE(enrolled_at) OVER (PARTITION BY course_id, term ORDER BY enrolled_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_enrolled_at,
        ROW_NUMBER() OVER (ORDER BY enrolled_at DESC, id) AS rn,
        DENSE_RANK() OVER w_all_by_enrolled_at_ties AS enrolled_at_dense_rank_2,
        LAG(enrolled_at) OVER (ORDER BY enrolled_at, id) AS prev_enrolled_at,
        LAG(enrolled_at) OVER (ORDER BY enrolled_at DESC, id) AS prev_enrolled_at_2,
        LEAD(enrolled_at) OVER (ORDER BY enrolled_at NULLS LAST, id) AS next_enrolled_at,
        ROUND((UNIX_TIMESTAMP(enrolled_at) - UNIX_TIMESTAMP(LAG(enrolled_at) OVER (ORDER BY enrolled_at, id))) / 60, 1) AS minutes_since_prev_enrolled_at_2,
        NTILE(4) OVER w_all_by_enrolled_at_3 AS enrolled_at_quartile
    FROM base_enrollments
    WINDOW w_all_by_enrolled_at AS (ORDER BY enrolled_at DESC, id), w_all_by_enrolled_at_ties AS (ORDER BY enrolled_at NULLS LAST), w_all_by_enrolled_at_3 AS (ORDER BY enrolled_at, id)
)
-- Final projection
SELECT
    id,
    enrolled_at,
    term_slug,
    prev2_enrolled_at,
    last_known_enrolled_at,
    enrolled_at_dense_rank,
    first_enrolled_at,
    rn,
    enrolled_at_dense_rank_2,
    prev_enrolled_at_2,
    next_enrolled_at,
    minutes_since_prev_enrolled_at_2,
    enrolled_at_quartile,
    enrolled_at + INTERVAL 7 DAYS AS enrolled_at_plus_7d
FROM metrics
ORDER BY enrolled_at DESC NULLS FIRST
