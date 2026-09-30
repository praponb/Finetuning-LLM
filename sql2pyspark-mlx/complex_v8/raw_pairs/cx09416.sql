WITH recent AS (
    -- Filter courses
    SELECT
        id,
        dept,
        credits,
        level,
        CONCAT_WS('-', dept, course_code) AS dept_course_code_label,
        UPPER(TRIM(dept)) AS dept_clean,
        LENGTH(TRIM(dept)) AS dept_len
    FROM courses
    WHERE dept = 'Biz'
),
metrics AS (
    -- Previous / next values per key
    SELECT
        *,
        ROUND(PERCENT_RANK() OVER (PARTITION BY level, dept_len ORDER BY credits, id), 3) AS credits_percent_rank,
        ROUND(dept_len / NULLIF(SUM(dept_len) OVER w_credits_dept_len, 0), 4) AS dept_len_share_of_credits_dept_len,
        ROUND(dept_len - AVG(dept_len) OVER w_credits_dept_len, 2) AS dept_len_vs_credits_dept_len_avg
    FROM recent
    WINDOW w_credits_dept_len AS (PARTITION BY credits, dept_len)
)
SELECT *
FROM (
    SELECT credits, level, credits_percent_rank
    FROM metrics
) src
PIVOT (
    MAX(credits_percent_rank)
    FOR level IN ('intro', 'core', 'advanced')
)
