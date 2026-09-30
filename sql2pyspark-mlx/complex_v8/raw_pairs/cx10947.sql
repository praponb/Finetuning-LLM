WITH base AS (
    -- Scope the students rows
    SELECT
        id,
        student_name,
        major,
        enrolled_on,
        gpa,
        is_international
    FROM students
    WHERE major = 'Biz'
),
latest AS (
    -- Keep the latest row per major
    SELECT
        id,
        student_name,
        major,
        enrolled_on,
        gpa,
        is_international
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY major ORDER BY enrolled_on DESC, id DESC) AS row_num
        FROM base
    ) ranked
    WHERE row_num = 1
)
SELECT *
FROM (
    SELECT enrolled_on, is_international, major, gpa
    FROM latest
) src
PIVOT (
    SUM(gpa)
    FOR major IN ('CS', 'Bio', 'Biz')
)
