SELECT region
FROM policyholders
WHERE segment = 'VIP'
INTERSECT ALL
SELECT holder_name AS region
FROM policyholders
WHERE holder_name IS NOT NULL
