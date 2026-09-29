WITH scoped AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        course_code,
        dept,
        level,
        INITCAP(TRIM(dept)) AS dept_clean,
        ABS(credits - 2) AS credits_distance,
        CASE WHEN credits >= 4 THEN 'HIGH' WHEN credits >= 2 THEN 'MEDIUM' ELSE 'LOW' END AS credits_tier
    FROM courses
    WHERE (UPPER(TRIM(course_code)) LIKE 'C%' OR level IN ('core', 'advanced'))
),
tagged AS (
    -- Label and score rows
    SELECT
        id,
        course_code,
        dept,
        level,
        dept_clean,
        credits_tier,
        ABS(credits_distance - 2) AS credits_distance_distance
    FROM scoped
    WHERE credits_distance >= 0
),
derived AS (
    -- Add calculated columns
    SELECT
        id,
        course_code,
        dept,
        dept_clean,
        credits_tier,
        credits_distance_distance,
        CASE WHEN level IN ('advanced', 'intro') THEN 'group_a' WHEN level = 'core' THEN 'group_b' ELSE 'other' END AS level_group,
        CASE WHEN credits_tier IN ('LOW', 'HIGH') THEN 'group_a' WHEN credits_tier = 'MEDIUM' THEN 'group_b' ELSE 'other' END AS credits_tier_group
    FROM tagged
    WHERE dept NOT IN ('Math', 'Art')
)
SELECT
    id,
    course_code,
    dept,
    dept_clean,
    credits_tier_group,
    CONCAT_WS('|', credits_tier_group, level_group) AS credits_tier_group_level_group_label
FROM derived
ORDER BY id DESC
