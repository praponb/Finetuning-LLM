SELECT country AS name, plan AS category, id
FROM listeners
WHERE SIZE(interests) >= 2
UNION ALL
SELECT country AS name, username AS category, id
FROM listeners
