SELECT
    plan AS category,
    signup_date,
    birth_year AS entity_id,
    'listener' AS source
FROM listeners
UNION ALL
SELECT
    username,
    signup_date,
    birth_year,
    'listener_2' AS source
FROM listeners
ORDER BY signup_date, category
