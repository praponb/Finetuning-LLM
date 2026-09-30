SELECT
    product AS name,
    end_date AS day,
    status AS name_2,
    'policy' AS origin
FROM policies
WHERE holder_id >= 6
  AND premium > 2013.8
UNION
SELECT
    segment AS name,
    joined_on AS day,
    region AS name_2,
    'holder' AS origin
FROM policyholders
WHERE segment IS NOT NULL
  AND YEAR(joined_on) = 2025
