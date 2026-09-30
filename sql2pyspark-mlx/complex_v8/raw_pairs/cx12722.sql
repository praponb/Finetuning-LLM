SELECT birth_year AS ref_id
FROM listeners
WHERE country NOT IN ('KR', 'JP')
EXCEPT
SELECT birth_year AS ref_id
FROM listeners
WHERE plan IN ('free', 'student')
  AND YEAR(signup_date) = 2024
ORDER BY ref_id
