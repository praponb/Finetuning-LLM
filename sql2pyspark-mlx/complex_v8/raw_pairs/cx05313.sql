WITH prepared AS (
    -- Filter listeners
    SELECT
        id,
        username,
        plan,
        birth_year,
        interests
    FROM listeners
    WHERE signup_date IS NOT NULL
),
derived AS (
    -- Label and score rows
    SELECT
        id,
        username,
        plan,
        birth_year,
        CASE WHEN plan IN ('student', 'premium') THEN 'group_a' WHEN plan = 'free' THEN 'group_b' ELSE 'other' END AS plan_group
    FROM prepared
),
stats AS (
    -- Summarise per group
    SELECT
        plan_group,
        plan,
        ARRAY_SORT(COLLECT_SET(plan)) AS plan_list,
        COUNT(*) AS row_count,
        COUNT(*) FILTER (WHERE UPPER(TRIM(username)) LIKE 'H%') AS n_upper_trim_username_like_h,
        COUNT(DISTINCT plan_group) AS distinct_plan_groups,
        COUNT(*) FILTER (WHERE UPPER(TRIM(username)) LIKE 'A%') AS n_upper_trim_username_like_a,
        COUNT(*) FILTER (WHERE username IS NOT NULL) AS n_username_is_not_null
    FROM derived
    GROUP BY plan_group, plan
)
-- Final projection
SELECT
    plan_group,
    plan,
    plan_list,
    row_count,
    n_upper_trim_username_like_h,
    distinct_plan_groups,
    n_username_is_not_null,
    n_username_is_not_null + row_count AS n_username_is_not_null_plus_row_count
FROM stats
WHERE plan IS NOT NULL
ORDER BY plan DESC, plan_group
LIMIT 10
