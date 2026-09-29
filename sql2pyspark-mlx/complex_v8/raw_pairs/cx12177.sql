WITH base_courses AS (
    -- Scope the courses rows
    SELECT
        id,
        course_code,
        dept,
        credits,
        level,
        CASE WHEN level IN ('core', 'intro') THEN 'group_a' WHEN level = 'advanced' THEN 'group_b' ELSE 'other' END AS level_group
    FROM courses
    WHERE dept = 'Art'
),
labeled AS (
    -- Derive helper columns
    SELECT *, COALESCE(dept, 'UNKNOWN') AS dept_or_unknown
    FROM base_courses
    WHERE UPPER(TRIM(course_code)) LIKE 'C%'
),
qualified AS (
    -- Compare against the overall avg
    SELECT *
    FROM labeled
    WHERE credits > (SELECT AVG(credits) FROM labeled)
),
annotated AS (
    -- Label and score rows
    SELECT *, CONCAT_WS('|', course_code, dept) AS course_code_dept_label
    FROM qualified
    WHERE LENGTH(TRIM(course_code)) >= 7
)
SELECT *
FROM (
    SELECT level, dept_or_unknown, level_group, credits
    FROM annotated
) src
PIVOT (
    SUM(credits) AS amount, COUNT(credits) AS n
    FOR level_group IN ('group_a', 'other')
)
