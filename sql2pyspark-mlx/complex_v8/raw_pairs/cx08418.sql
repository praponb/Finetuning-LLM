WITH base AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        course_code,
        dept,
        credits,
        level,
        CASE WHEN dept IN ('Biz', 'CS') THEN 'group_a' WHEN dept = 'Math' THEN 'group_b' ELSE 'other' END AS dept_group,
        LENGTH(TRIM(dept)) AS dept_len
    FROM courses
),
with_windows AS (
    -- Window metrics over several partitions
    SELECT
        id,
        course_code,
        dept,
        dept_group,
        dept_len,
        LAG(dept_len, 1, 0) OVER (PARTITION BY dept_len ORDER BY credits NULLS LAST, id) AS prev_dept_len,
        MIN(credits) OVER (PARTITION BY dept_len, dept_group) AS dept_len_dept_group_min_credits
    FROM base
),
enriched AS (
    -- Label and score rows
    SELECT *, COALESCE(dept_group, 'UNKNOWN') AS dept_group_or_unknown
    FROM with_windows
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT dept_len, dept_group_or_unknown, dept, prev_dept_len
    FROM enriched
) src
PIVOT (
    MIN(prev_dept_len)
    FOR dept IN ('Art', 'CS')
)
ORDER BY dept_len, dept_group_or_unknown
