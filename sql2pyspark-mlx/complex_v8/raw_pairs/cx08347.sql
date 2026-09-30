WITH valid AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        username,
        plan,
        birth_year,
        interests,
        DATE_FORMAT(signup_date, 'MMM yyyy') AS signup_date_label
    FROM listeners
),
labeled AS (
    -- Label and score rows
    SELECT
        *,
        LOWER(REGEXP_REPLACE(TRIM(plan), ' ', '_')) AS plan_slug,
        COALESCE(plan, 'UNKNOWN') AS plan_or_unknown
    FROM valid
),
tagged AS (
    -- Label and score rows
    SELECT
        *,
        ARRAY_CONTAINS(interests, 'kpop') AS has_kpop,
        LENGTH(TRIM(plan)) AS plan_len,
        INITCAP(TRIM(username)) AS username_clean
    FROM labeled
)
SELECT *
FROM (
    SELECT has_kpop, plan_or_unknown, birth_year
    FROM tagged
) src
PIVOT (
    SUM(birth_year)
    FOR plan_or_unknown IN ('free', 'family', 'UNKNOWN', 'student')
)
ORDER BY has_kpop
