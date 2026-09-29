SELECT birth_year AS entity_id, signup_date, plan
FROM listeners
WHERE plan NOT IN ('premium', 'student')
  AND LOWER(username) LIKE '%a.%'
UNION
SELECT birth_year, signup_date, plan
FROM listeners
ORDER BY signup_date DESC
