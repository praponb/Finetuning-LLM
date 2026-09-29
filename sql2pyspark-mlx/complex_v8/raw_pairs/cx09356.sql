WITH cleaned_courses AS (
    -- Filter courses
    SELECT
        id,
        course_code,
        dept,
        credits,
        level,
        CONCAT_WS(' / ', course_code, level) AS course_code_level_label,
        COALESCE(dept, 'UNKNOWN') AS dept_or_unknown
    FROM courses
    WHERE LOWER(course_code) LIKE '%30%'
),
latest AS (
    -- Keep the latest row per level
    SELECT
        id,
        course_code,
        dept,
        credits,
        level,
        course_code_level_label,
        dept_or_unknown
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY level ORDER BY credits DESC, id DESC) AS row_num
        FROM cleaned_courses
    ) ranked
    WHERE row_num = 1
)
SELECT
    dept,
    level,
    ARRAY_SORT(COLLECT_SET(course_code)) AS course_code_list,
    COUNT(DISTINCT level) AS distinct_levels,
    MIN(credits) AS min_credits
FROM latest
GROUP BY GROUPING SETS ((dept, level), (dept))
ORDER BY dept NULLS LAST, level NULLS LAST
