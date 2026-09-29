SELECT region AS category, joined_on
FROM policyholders
UNION ALL
SELECT product, start_date
FROM policies
UNION ALL
SELECT holder_name, joined_on
FROM policyholders
WHERE region IS NOT NULL
