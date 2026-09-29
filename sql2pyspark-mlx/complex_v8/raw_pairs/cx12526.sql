WITH cleaned_listeners AS (
    -- Scope the listeners rows
    SELECT
        id,
        username,
        country,
        signup_date,
        interests,
        COALESCE(country, 'UNKNOWN') AS country_or_unknown,
        CASE WHEN birth_year >= 1994 THEN 'HIGH' WHEN birth_year >= 1968 THEN 'MEDIUM' ELSE 'LOW' END AS birth_year_tier,
        CONCAT_WS('-', plan, username) AS plan_username_label
    FROM listeners
),
tagged AS (
    -- Add calculated columns
    SELECT
        id,
        username,
        country,
        interests,
        plan_username_label,
        DATE_FORMAT(signup_date, 'yyyy-MM') AS signup_date_label
    FROM cleaned_listeners
)
-- Final projection
SELECT
    id,
    username,
    country,
    plan_username_label,
    signup_date_label,
    CONCAT_WS('-', signup_date_label, country) AS signup_date_label_country_label,
    CONCAT_WS(' / ', plan_username_label, signup_date_label) AS plan_username_label_signup_date_label_label
FROM tagged
WHERE LENGTH(TRIM(username)) >= 7
ORDER BY signup_date_label_country_label NULLS LAST, id
LIMIT 5
