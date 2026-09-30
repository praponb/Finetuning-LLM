WITH base AS (
    -- Filter courses
    SELECT
        id,
        course_code,
        dept,
        credits
    FROM courses
    WHERE (dept = 'Biz' OR LENGTH(TRIM(course_code)) >= 5)
),
latest AS (
    -- Keep the latest row per dept
    SELECT
        id,
        course_code,
        dept,
        credits
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY dept ORDER BY credits DESC, id DESC) AS row_num
        FROM base
    ) ranked
    WHERE row_num = 1
),
labeled AS (
    -- Label and score rows
    SELECT
        id,
        credits,
        CONCAT_WS(': ', dept, course_code) AS dept_course_code_label,
        COALESCE(dept, 'UNKNOWN') AS dept_or_unknown,
        COALESCE(credits, 0) AS credits_filled
    FROM latest
)
SELECT
    id,
    credits,
    dept_course_code_label,
    dept_or_unknown,
    credits_filled,
    LENGTH(TRIM(dept_or_unknown)) AS dept_or_unknown_len,
    GREATEST(credits, credits_filled) AS greatest_credits_credits_filled
FROM labeled
ORDER BY credits_filled DESC, greatest_credits_credits_filled DESC
