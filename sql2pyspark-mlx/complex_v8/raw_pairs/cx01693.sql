WITH filtered AS (
    -- Filter courses
    SELECT
        id,
        course_code,
        dept,
        credits,
        CONCAT_WS('|', level, dept) AS level_dept_label
    FROM courses
    WHERE level NOT IN ('intro', 'advanced')
),
with_activity AS (
    -- Rows that have matching enrollments
    SELECT s.*
    FROM filtered s
    WHERE EXISTS (
            SELECT 1
            FROM enrollments e
            WHERE e.course_id = s.id
        )
),
sequenced AS (
    -- Running and moving aggregates
    SELECT
        *,
        MIN(credits) OVER (PARTITION BY credits) AS credits_min_credits,
        ROUND(PERCENT_RANK() OVER (ORDER BY credits DESC), 3) AS credits_percent_rank,
        ROUND(credits - AVG(credits) OVER (PARTITION BY credits), 2) AS credits_vs_credits_avg,
        LEAD(credits, 2) OVER (ORDER BY credits DESC, id) AS next2_credits,
        ROUND(credits / NULLIF(SUM(credits) OVER w_credits, 0), 4) AS credits_share_of_credits,
        LAST_VALUE(credits) OVER (ORDER BY credits DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_credits
    FROM with_activity
    WINDOW w_credits AS (PARTITION BY credits)
),
tagged AS (
    -- Label and score rows
    SELECT
        id,
        course_code,
        dept,
        credits,
        level_dept_label,
        credits_min_credits,
        credits_percent_rank,
        credits_vs_credits_avg,
        next2_credits,
        GREATEST(credits_share_of_credits, last_credits) AS greatest_credits_share_of_credits_last_credits,
        FLOOR(credits_min_credits / 5) * 5 AS credits_min_credits_bucket,
        CONCAT(course_code, '#', CAST(credits AS STRING)) AS course_code_ref
    FROM sequenced
)
SELECT
    next2_credits,
    dept,
    MAX(next2_credits) AS max_next2_credits,
    ARRAY_SORT(COLLECT_SET(course_code)) AS course_code_list
FROM tagged
GROUP BY GROUPING SETS ((next2_credits, dept), (next2_credits), (dept))
ORDER BY next2_credits NULLS LAST, dept NULLS LAST
