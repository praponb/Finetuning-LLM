WITH cleaned_courses AS (
    -- Scope the courses rows
    SELECT
        id,
        dept,
        credits,
        level,
        LOWER(REGEXP_REPLACE(TRIM(course_code), ' ', '_')) AS course_code_slug,
        CASE WHEN dept IN ('Biz', 'Art') THEN 'group_a' WHEN dept = 'Bio' THEN 'group_b' ELSE 'other' END AS dept_group
    FROM courses
    WHERE credits > 2
),
labeled AS (
    -- Label and score rows
    SELECT
        *,
        LENGTH(TRIM(dept_group)) AS dept_group_len,
        CONCAT_WS(' / ', dept_group, level) AS dept_group_level_label,
        LOWER(TRIM(course_code_slug)) AS course_code_slug_clean
    FROM cleaned_courses
)
-- Subtotals with CUBE
SELECT
    dept,
    dept_group_len,
    SUM(dept_group_len) AS total_dept_group_len
FROM labeled
GROUP BY CUBE(dept, dept_group_len)
