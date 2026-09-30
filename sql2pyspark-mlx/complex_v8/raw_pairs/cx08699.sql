SELECT product
FROM policies
INTERSECT
SELECT status
FROM claims
ORDER BY product DESC
