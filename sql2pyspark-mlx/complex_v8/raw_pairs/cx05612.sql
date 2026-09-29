WITH filtered AS (
    -- Scope the courses rows
    SELECT
        id,
        course_code,
        credits,
        level,
        CASE WHEN credits >= 3 THEN 'HIGH' WHEN credits >= 2 THEN 'MEDIUM' ELSE 'LOW' END AS credits_tier,
        SUBSTRING(TRIM(dept), 1, 4) AS dept_prefix,
        COALESCE(level, 'UNKNOWN') AS level_or_unknown
    FROM courses
    WHERE credits < 3
      AND dept = 'Math'
),
metrics AS (
    -- Previous / next values per key
    SELECT
        id,
        course_code,
        credits,
        level,
        credits_tier,
        dept_prefix,
        level_or_unknown,
        ROUND(credits / NULLIF(SUM(credits) OVER (PARTITION BY level_or_unknown), 0), 4) AS credits_share_of_level_or_unknown,
        LAG(credits) OVER (PARTITION BY level_or_unknown ORDER BY credits ASC, id) AS prev_credits,
        SUM(credits) OVER (PARTITION BY level_or_unknown) AS level_or_unknown_sum_credits
    FROM filtered
),
scored AS (
    -- Label and score rows
    SELECT
        id,
        credits,
        credits_tier,
        credits_share_of_level_or_unknown,
        level_or_unknown_sum_credits,
        CONCAT(dept_prefix, '#', CAST(prev_credits AS STRING)) AS dept_prefix_ref,
        CASE WHEN credits >= 3 THEN 'HIGH' WHEN credits >= 2 THEN 'MEDIUM' ELSE 'LOW' END AS credits_tier_2,
        CONCAT(level, '#', CAST(prev_credits AS STRING)) AS level_ref
    FROM metrics
)
SELECT
    id,
    credits_tier,
    credits_share_of_level_or_unknown,
    level_or_unknown_sum_credits,
    credits_tier_2,
    level_ref,
    ABS(credits - 3) AS credits_distance
FROM scored
ORDER BY credits_tier_2 DESC, id
