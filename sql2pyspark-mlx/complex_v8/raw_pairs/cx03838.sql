SELECT
    'listener' AS src,
    username AS name,
    signup_date AS event_date,
    country
FROM listeners
UNION
SELECT
    'listener_2' AS src,
    plan,
    signup_date,
    country
FROM listeners
