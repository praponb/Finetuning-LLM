WITH filtered_courses AS (
    -- Filter courses
    SELECT
        id,
        dept,
        credits,
        level
    FROM courses
    WHERE LENGTH(TRIM(course_code)) >= 5
      AND credits <= 4
),
sequenced AS (
    -- Running and moving aggregates
    SELECT
        id,
        dept,
        level,
        ROUND(PERCENT_RANK() OVER (PARTITION BY level, credits ORDER BY credits ASC), 3) AS credits_percent_rank,
        FIRST_VALUE(credits) IGNORE NULLS OVER (PARTITION BY level ORDER BY credits, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_credits,
        DENSE_RANK() OVER (PARTITION BY level, credits ORDER BY credits ASC) AS credits_dense_rank,
        LAG(credits) OVER (PARTITION BY level, credits ORDER BY credits ASC, id) AS prev_credits
    FROM filtered_courses
),
qualified AS (
    -- Compare against the overall min
    SELECT *
    FROM sequenced
    WHERE prev_credits > (SELECT MIN(prev_credits) FROM sequenced)
),
leaders AS (
    -- First row per group
    SELECT *
    FROM qualified
    WHERE credits_dense_rank = 1
),
qualified_2 AS (
    -- Compare against the overall min
    SELECT *
    FROM leaders
    WHERE first_known_credits > (SELECT MIN(first_known_credits) FROM leaders)
)
SELECT
    id,
    dept,
    first_known_credits,
    credits_dense_rank,
    LEAST(credits_percent_rank, credits_dense_rank) AS least_credits_percent_rank_credits_dense_rank,
    LEAST(first_known_credits, credits_percent_rank) AS least_first_known_credits_credits_percent_rank
FROM qualified_2
WHERE credits_percent_rank > 0.3
ORDER BY dept, credits_dense_rank DESC, id
LIMIT 20
