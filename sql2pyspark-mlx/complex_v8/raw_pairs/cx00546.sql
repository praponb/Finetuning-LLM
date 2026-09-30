SELECT 'listener' AS record_type, country, signup_date
FROM listeners
UNION ALL
SELECT 'listener_2' AS record_type, plan AS country, signup_date
FROM listeners
