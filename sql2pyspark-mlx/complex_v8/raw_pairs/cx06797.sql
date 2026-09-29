SELECT product AS category, status, 'policy' AS src
FROM policies
UNION ALL
SELECT holder_name AS category, region AS status, 'holder' AS src
FROM policyholders
WHERE region <> 'West'
UNION ALL
SELECT status AS category, product AS status, 'policy_3' AS src
FROM policies
ORDER BY category, status
