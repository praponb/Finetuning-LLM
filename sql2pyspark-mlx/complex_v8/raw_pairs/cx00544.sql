WITH cleaned_courses AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        course_code,
        credits,
        level,
        LOWER(REGEXP_REPLACE(TRIM(dept), ' ', '_')) AS dept_slug
    FROM courses
    WHERE level = 'core'
      AND UPPER(TRIM(course_code)) LIKE 'C%'
),
scored AS (
    -- Label and score rows
    SELECT
        *,
        CONCAT_WS('-', course_code, dept_slug) AS course_code_dept_slug_label,
        LENGTH(TRIM(dept_slug)) AS dept_slug_len
    FROM cleaned_courses
),
unique_rows AS (
    -- Keep the latest row per level
    SELECT
        id,
        course_code,
        credits,
        level,
        dept_slug,
        course_code_dept_slug_label,
        dept_slug_len
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY level ORDER BY credits DESC, id DESC) AS recency_rank
        FROM scored
    ) ranked
    WHERE recency_rank = 1
),
with_activity AS (
    -- Compare against the overall min
    SELECT *
    FROM unique_rows
    WHERE credits > (SELECT MIN(credits) FROM unique_rows)
)
SELECT
    level,
    dept_slug_len,
    GROUPING_ID() AS grouping_level,
    SUM(CASE WHEN course_code IS NOT NULL THEN credits ELSE 0 END) AS credits_course_code_is_not_null
FROM with_activity
GROUP BY GROUPING SETS ((level, dept_slug_len), (level))
ORDER BY level NULLS LAST, dept_slug_len NULLS LAST
