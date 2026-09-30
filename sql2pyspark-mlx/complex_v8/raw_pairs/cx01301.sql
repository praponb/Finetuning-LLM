WITH cleaned AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        course_code,
        dept,
        credits,
        ABS(credits - 2) AS credits_distance,
        ABS(credits - 2) AS credits_distance_2,
        COALESCE(level, 'UNKNOWN') AS level_or_unknown
    FROM courses
    WHERE credits BETWEEN 2 AND 3
      AND level = 'core'
),
scored AS (
    -- Add calculated columns
    SELECT
        id,
        course_code,
        dept,
        credits,
        credits_distance_2,
        level_or_unknown,
        LEAST(credits_distance, credits_distance_2) AS least_credits_distance_credits_distance_2,
        credits_distance - credits AS credits_distance_minus_credits
    FROM cleaned
),
derived AS (
    -- Derive helper columns
    SELECT
        *,
        LEAST(credits, credits_distance_2) AS least_credits_credits_distance_2
    FROM scored
)
-- Final projection
SELECT
    id,
    course_code,
    dept,
    credits_distance_2,
    level_or_unknown,
    credits_distance_minus_credits,
    least_credits_credits_distance_2
FROM derived
ORDER BY credits_distance_2 DESC NULLS FIRST, least_credits_credits_distance_2
