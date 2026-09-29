WITH base AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        course_code,
        dept,
        credits,
        UPPER(TRIM(level)) AS level_clean,
        CASE WHEN dept IN ('Art', 'CS') THEN 'group_a' WHEN dept = 'Bio' THEN 'group_b' ELSE 'other' END AS dept_group
    FROM courses
    WHERE dept = 'Bio'
),
scored AS (
    -- Derive helper columns
    SELECT
        *,
        CONCAT_WS('|', dept_group, level_clean) AS dept_group_level_clean_label
    FROM base
    WHERE UPPER(TRIM(level_clean)) LIKE 'I%'
),
totals AS (
    -- Group-level totals
    SELECT
        dept_group,
        ARRAY_SORT(COLLECT_SET(course_code)) AS course_code_list,
        MAX(credits) AS max_credits
    FROM scored
    GROUP BY dept_group
    HAVING MAX(credits) > 2
),
with_trends AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(PERCENT_RANK() OVER w_all_by_max_credits_ties, 3) AS max_credits_percent_rank,
        SIZE(COLLECT_SET(dept_group) OVER (PARTITION BY max_credits)) AS distinct_dept_group_in_max_credits,
        ROW_NUMBER() OVER w_all_by_max_credits AS seq_no,
        LAST_VALUE(max_credits) IGNORE NULLS OVER (ORDER BY max_credits ASC, dept_group ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_max_credits
    FROM totals
    WINDOW w_all_by_max_credits_ties AS (ORDER BY max_credits ASC), w_all_by_max_credits AS (ORDER BY max_credits ASC, dept_group)
)
-- Native SQL UNPIVOT (wide to long)
SELECT dept_group, metric, amount
FROM (
    SELECT dept_group, distinct_dept_group_in_max_credits, max_credits, last_known_max_credits
    FROM with_trends
) src
UNPIVOT (
    amount FOR metric IN (distinct_dept_group_in_max_credits AS m_distinct_dept_group_in_max_credits, max_credits AS max_credits_v, last_known_max_credits AS m_last_known_max_credits)
) unp
