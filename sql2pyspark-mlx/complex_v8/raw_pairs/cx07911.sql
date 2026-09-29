WITH valid AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        course_code,
        dept,
        credits
    FROM courses
    WHERE level <> 'core'
      AND dept NOT IN ('Math', 'Bio')
),
scored AS (
    -- Add calculated columns
    SELECT
        id,
        course_code,
        LOWER(REGEXP_REPLACE(TRIM(dept), ' ', '_')) AS dept_slug,
        CONCAT_WS(' / ', dept, course_code) AS dept_course_code_label,
        COALESCE(credits, 0) AS credits_filled
    FROM valid
)
SELECT DISTINCT dept_course_code_label, id, course_code
FROM scored
ORDER BY course_code, id
