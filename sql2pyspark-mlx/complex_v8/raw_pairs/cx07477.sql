SELECT country AS label
FROM listeners
WHERE birth_year <= 1990
  AND signup_date BETWEEN DATE '2024-04-16' AND DATE '2025-05-05'
EXCEPT ALL
SELECT country
FROM listeners
WHERE ARRAY_CONTAINS(interests, 'podcasts')
ORDER BY label
