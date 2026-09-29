WITH cleaned AS (
    -- Scope the listeners rows
    SELECT
        id,
        username,
        plan,
        signup_date,
        birth_year,
        interests
    FROM listeners
),
tagged AS (
    -- Derive helper columns
    SELECT
        *,
        UPPER(TRIM(plan)) AS plan_clean,
        LOWER(REGEXP_REPLACE(TRIM(plan), ' ', '_')) AS plan_slug,
        DATE_ADD(signup_date, 365) AS signup_date_plus_365d
    FROM cleaned
    WHERE UPPER(TRIM(username)) LIKE 'F%'
),
annotated AS (
    -- Derive helper columns
    SELECT
        *,
        CONCAT(plan, '#', CAST(id AS STRING)) AS plan_ref,
        FLOOR(birth_year / 10) * 10 AS birth_year_bucket
    FROM tagged
    WHERE signup_date >= DATE '2024-10-07'
)
-- Final projection
SELECT
    id,
    username,
    signup_date,
    birth_year,
    interests,
    plan_clean,
    plan_slug,
    signup_date_plus_365d,
    plan_ref,
    birth_year_bucket,
    DATE_FORMAT(signup_date_plus_365d, 'yyyy-MM-dd') AS signup_date_plus_365d_label
FROM annotated
WHERE birth_year < 1980
ORDER BY username, id
LIMIT 10
