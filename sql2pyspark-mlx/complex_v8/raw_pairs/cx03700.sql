WITH filtered_courses AS (
    -- Filter courses
    SELECT
        id,
        course_code,
        credits,
        level,
        LOWER(REGEXP_REPLACE(TRIM(level), ' ', '_')) AS level_slug
    FROM courses
    WHERE level IN ('advanced', 'intro')
      AND credits > 2
),
filtered AS (
    -- Rows that have matching enrollments
    SELECT s.*
    FROM filtered_courses s
    WHERE EXISTS (
            SELECT 1
            FROM enrollments e
            WHERE e.course_id = s.id
        )
),
windowed AS (
    -- Previous / next values per key
    SELECT
        *,
        RANK() OVER (ORDER BY credits DESC, id) AS credits_rank,
        MIN(credits) OVER (PARTITION BY level) AS level_min_credits,
        COUNT(*) OVER (PARTITION BY level) AS rows_in_level
    FROM filtered
),
windowed_2 AS (
    -- Running and moving aggregates
    SELECT
        *,
        LEAD(rows_in_level) OVER (ORDER BY rows_in_level DESC, id) AS next_rows_in_level,
        DENSE_RANK() OVER (ORDER BY rows_in_level DESC) AS rows_in_level_dense_rank
    FROM windowed
)
-- Subtotals with CUBE
SELECT
    COALESCE(level, 'ALL') AS level,
    rows_in_level_dense_rank,
    GROUPING(level) AS is_total_level,
    ARRAY_SORT(COLLECT_SET(level)) AS level_list,
    ROUND(SUM(credits) / NULLIF(SUM(next_rows_in_level), 0), 3) AS credits_to_next_rows_in_level_ratio
FROM windowed_2
GROUP BY CUBE(level, rows_in_level_dense_rank)
ORDER BY level NULLS LAST, rows_in_level_dense_rank NULLS LAST
