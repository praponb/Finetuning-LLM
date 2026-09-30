WITH valid AS (
    -- Filter courses
    SELECT
        id,
        course_code,
        dept,
        credits,
        LENGTH(TRIM(course_code)) AS course_code_len
    FROM courses
    WHERE level NOT IN ('intro', 'core')
),
deduped AS (
    -- Keep the latest row per dept
    SELECT
        id,
        course_code,
        dept,
        credits,
        course_code_len
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY dept ORDER BY course_code_len DESC, id DESC) AS recency_rank
        FROM valid
    ) ranked
    WHERE recency_rank = 1
),
scored AS (
    -- Label and score rows
    SELECT
        *,
        CONCAT_WS('-', dept, course_code) AS dept_course_code_label,
        SUBSTRING(TRIM(dept), 1, 2) AS dept_prefix
    FROM deduped
)
-- Final projection
SELECT
    id,
    course_code,
    dept,
    course_code_len,
    dept_prefix,
    CASE WHEN credits >= 4 THEN 'large' WHEN credits >= 2 THEN 'medium' ELSE 'small' END AS credits_tier
FROM scored
ORDER BY dept DESC
