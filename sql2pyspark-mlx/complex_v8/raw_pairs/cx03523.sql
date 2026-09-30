SELECT segment AS label
FROM policyholders
WHERE YEAR(joined_on) = 2024
INTERSECT
SELECT product AS label
FROM policies
