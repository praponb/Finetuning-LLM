SELECT id, product
FROM policies
WHERE YEAR(end_date) = 2025
EXCEPT
SELECT level, adjuster_name
FROM adjusters
WHERE level > 2
