WITH recent AS (
    -- Filter courses
    SELECT
        id,
        dept,
        credits,
        level,
        CASE WHEN credits >= 3 THEN 'large' WHEN credits >= 2 THEN 'medium' ELSE 'small' END AS credits_tier,
        CONCAT_WS('-', level, dept) AS level_dept_label,
        LOWER(REGEXP_REPLACE(TRIM(level), ' ', '_')) AS level_slug
    FROM courses
    WHERE dept = 'Art'
      AND credits >= 2
),
latest AS (
    -- Keep the earliest row per credits_tier, dept
    SELECT
        id,
        dept,
        credits,
        level,
        credits_tier,
        level_dept_label,
        level_slug
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY credits_tier, dept ORDER BY credits, id) AS recency_rank
        FROM recent
    ) ranked
    WHERE recency_rank = 1
),
metrics AS (
    -- Previous / next values per key
    SELECT
        *,
        DENSE_RANK() OVER w_credits_by_credits AS credits_dense_rank,
        SUM(credits) OVER (PARTITION BY credits, level) AS credits_level_sum_credits,
        MIN(credits) OVER (PARTITION BY credits, level) AS credits_level_min_credits,
        LAG(credits, 3) OVER (PARTITION BY dept ORDER BY credits DESC, id) AS prev3_credits
    FROM latest
    WINDOW w_credits_by_credits AS (PARTITION BY credits ORDER BY credits DESC NULLS FIRST, id), w_credits_level AS (PARTITION BY credits, level)
),
unique_rows AS (
    -- Keep the latest row per level, dept
    SELECT
        id,
        dept,
        credits,
        level,
        credits_tier,
        level_dept_label,
        level_slug,
        credits_dense_rank,
        credits_level_sum_credits,
        credits_level_min_credits,
        prev3_credits
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY level, dept ORDER BY credits DESC, id DESC) AS recency_rank
        FROM metrics
    ) ranked
    WHERE recency_rank = 1
),
ranked AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        NTILE(4) OVER w_all_by_credits_level_min_cre AS credits_level_min_credits_quartile,
        DENSE_RANK() OVER (PARTITION BY dept ORDER BY credits_dense_rank DESC, id) AS credits_dense_rank_dense_rank,
        ROUND(CUME_DIST() OVER w_all_by_credits_level_min_cre, 3) AS credits_level_min_credits_cume_dist
    FROM unique_rows
    WINDOW w_all_by_credits_level_min_cre AS (ORDER BY credits_level_min_credits, id)
),
dept_stats AS (
    -- Max credits_level_min_credits_quartile per dept
    SELECT dept, MAX(credits_level_min_credits_quartile) AS max_credits_level_min_credits_quartile
    FROM ranked
    GROUP BY dept
),
above_average AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.max_credits_level_min_credits_quartile,
        ROUND(s.credits_level_min_credits_quartile - g.max_credits_level_min_credits_quartile, 2) AS credits_level_min_credits_quartile_minus_max
    FROM ranked s
    JOIN dept_stats g ON g.dept = s.dept
    WHERE s.credits_level_min_credits_quartile = g.max_credits_level_min_credits_quartile
)
SELECT *
FROM (
    SELECT credits_level_min_credits_quartile, credits_dense_rank, level, credits_level_min_credits
    FROM above_average
) src
PIVOT (
    MAX(credits_level_min_credits)
    FOR level IN ('intro', 'core')
)
