SELECT product AS name, end_date AS day, 'policy' AS src
FROM policies
WHERE status <> 'CANCELLED'
UNION ALL
SELECT product, end_date, 'policy_2' AS src
FROM policies
WHERE holder_id IS NOT NULL
  AND premium >= 2096.6
UNION ALL
SELECT holder_name, joined_on, 'holder' AS src
FROM policyholders
ORDER BY src DESC
