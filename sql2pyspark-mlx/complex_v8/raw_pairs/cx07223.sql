SELECT username AS label, country AS label_2, signup_date AS day
FROM listeners
UNION ALL
SELECT country, username, signup_date
FROM listeners
ORDER BY label_2
