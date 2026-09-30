WITH valid AS (
    -- Filter courses
    SELECT
        id,
        course_code,
        dept,
        credits,
        level,
        UPPER(TRIM(dept)) AS dept_clean,
        ABS(credits - 2) AS credits_distance
    FROM courses
    WHERE dept = 'CS'
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        COUNT(*) OVER (PARTITION BY credits, credits_distance) AS rows_in_credits_credits_distance,
        FIRST_VALUE(credits) OVER (PARTITION BY level, credits ORDER BY credits_distance, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_credits
    FROM valid
),
sequenced AS (
    -- Window metrics over several partitions
    SELECT
        id,
        course_code,
        dept,
        level,
        dept_clean,
        credits_distance,
        rows_in_credits_credits_distance,
        first_credits,
        COUNT(*) OVER w_dept_credits_distance AS rows_in_dept_credits_distance,
        ROUND(CUME_DIST() OVER w_all_by_first_credits_ties, 3) AS first_credits_cume_dist,
        MAX(credits_distance) OVER (PARTITION BY dept, credits_distance) AS dept_credits_distance_max_credits_distance
    FROM metrics
    WINDOW w_dept_credits_distance AS (PARTITION BY dept, credits_distance), w_all_by_first_credits_ties AS (ORDER BY first_credits NULLS LAST)
)
-- Final projection
SELECT
    id,
    course_code,
    level,
    dept_clean,
    credits_distance,
    rows_in_credits_credits_distance,
    first_credits,
    first_credits_cume_dist,
    dept_credits_distance_max_credits_distance
FROM sequenced
