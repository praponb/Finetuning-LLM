SELECT plan AS name, signup_date, country AS name_2
FROM listeners
WHERE birth_year > 1966
UNION
SELECT country AS name, signup_date, plan AS name_2
FROM listeners
WHERE SIZE(interests) >= 2
  AND username IS NOT NULL
