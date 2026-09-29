WITH valid_courses AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        course_code,
        dept,
        credits,
        level,
        ABS(credits - 3) AS credits_distance
    FROM courses
    WHERE UPPER(TRIM(course_code)) LIKE 'C%'
      AND credits >= 2
),
enriched AS (
    -- Label and score rows
    SELECT
        id,
        level,
        credits_distance,
        ABS(credits_distance - 2) AS credits_distance_distance,
        CASE WHEN credits >= 3 THEN 'large' WHEN credits >= 2 THEN 'medium' ELSE 'small' END AS credits_tier,
        INITCAP(TRIM(level)) AS level_clean
    FROM valid_courses
    WHERE credits_distance <= 3
),
latest AS (
    -- Keep the latest row per level
    SELECT
        id,
        level,
        credits_distance,
        credits_distance_distance,
        credits_tier,
        level_clean
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY level ORDER BY credits_distance DESC, id DESC) AS recency_rank
        FROM enriched
    ) ranked
    WHERE recency_rank = 1
)
-- Subtotals with ROLLUP
SELECT
    credits_distance,
    credits_distance_distance,
    COUNT(DISTINCT id) AS distinct_ids,
    ROUND(STDDEV(credits_distance), 2) AS stddev_credits_distance
FROM latest
GROUP BY ROLLUP(credits_distance, credits_distance_distance)
ORDER BY credits_distance NULLS LAST, credits_distance_distance NULLS LAST
