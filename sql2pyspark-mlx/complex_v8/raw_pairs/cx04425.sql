SELECT birth_year, signup_date, 'listener' AS src
FROM listeners
WHERE YEAR(signup_date) = 2024
  AND birth_year < 1994
UNION ALL
SELECT birth_year, signup_date, 'listener_2' AS src
FROM listeners
WHERE ARRAY_CONTAINS(interests, 'indie')
UNION ALL
SELECT birth_year, signup_date, 'listener_3' AS src
FROM listeners
ORDER BY birth_year DESC
