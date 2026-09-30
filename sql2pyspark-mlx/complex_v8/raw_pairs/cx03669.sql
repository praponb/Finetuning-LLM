WITH scoped AS (
    -- Filter courses
    SELECT
        id,
        course_code,
        dept,
        credits,
        level,
        CASE WHEN level IN ('core', 'intro') THEN 'group_a' WHEN level = 'advanced' THEN 'group_b' ELSE 'other' END AS level_group,
        COALESCE(credits, 0) AS credits_filled
    FROM courses
    WHERE LENGTH(TRIM(course_code)) >= 7
      AND dept NOT IN ('Art', 'Biz')
),
with_windows AS (
    -- Window metrics over several partitions
    SELECT
        id,
        course_code,
        dept,
        credits,
        level,
        credits_filled,
        SUM(credits) OVER w_credits AS credits_sum_credits,
        MIN(credits) OVER (PARTITION BY credits) AS credits_min_credits
    FROM scoped
    WINDOW w_credits AS (PARTITION BY credits)
),
totals AS (
    -- Group-level totals
    SELECT
        credits_filled,
        credits,
        ROUND(SUM(credits_min_credits) / NULLIF(SUM(credits_filled), 0), 3) AS credits_min_credits_to_credits_filled_ratio,
        ROUND(AVG(credits), 2) AS avg_credits,
        SUM(credits_sum_credits) AS total_credits_sum_credits,
        COUNT(DISTINCT level) AS distinct_levels,
        ROUND(SUM(credits_min_credits) / NULLIF(SUM(credits_sum_credits), 0), 3) AS credits_min_credits_to_credits_sum_credits_ratio
    FROM with_windows
    WHERE UPPER(TRIM(course_code)) LIKE 'C%'
    GROUP BY credits_filled, credits
    HAVING MAX(credits_filled) > 2
),
enriched AS (
    -- Derive helper columns
    SELECT
        credits_filled,
        credits,
        credits_min_credits_to_credits_filled_ratio,
        avg_credits,
        distinct_levels,
        credits_min_credits_to_credits_sum_credits_ratio,
        ROUND(avg_credits * 1.1, 2) AS avg_credits_adj
    FROM totals
    WHERE distinct_levels > 5
),
annotated AS (
    -- Label and score rows
    SELECT
        *,
        LEAST(avg_credits_adj, avg_credits) AS least_avg_credits_adj_avg_credits
    FROM enriched
    WHERE credits <= 4
),
with_windows_2 AS (
    -- Running and moving aggregates
    SELECT
        *,
        LEAD(distinct_levels, 2) OVER (PARTITION BY credits_filled ORDER BY least_avg_credits_adj_avg_credits, credits_filled, credits) AS next2_distinct_levels,
        LEAD(avg_credits_adj) OVER (PARTITION BY credits_filled ORDER BY least_avg_credits_adj_avg_credits, credits_filled, credits) AS next_avg_credits_adj,
        avg_credits - LAG(avg_credits) OVER (PARTITION BY credits_filled, distinct_levels ORDER BY distinct_levels DESC, credits_filled, credits) AS avg_credits_change,
        COUNT(*) OVER (PARTITION BY credits_filled) AS rows_in_credits_filled,
        ROUND(avg_credits / NULLIF(SUM(avg_credits) OVER (PARTITION BY credits_filled), 0), 4) AS avg_credits_share_of_credits_filled
    FROM annotated
)
SELECT credits_filled, credits, field, metric_value
FROM (
    SELECT credits_filled, credits, avg_credits_change, credits_min_credits_to_credits_filled_ratio, avg_credits
    FROM with_windows_2
) src
UNPIVOT (
    metric_value FOR field IN (avg_credits_change, credits_min_credits_to_credits_filled_ratio, avg_credits)
) unp
