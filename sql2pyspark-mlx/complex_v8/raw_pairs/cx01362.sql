SELECT
    'policy' AS src,
    product AS name,
    end_date AS day,
    id
FROM policies
WHERE start_date < DATE '2025-02-14'
  AND product = 'HEALTH'
UNION ALL
SELECT
    'claim' AS src,
    status AS name,
    claim_date AS day,
    id
FROM claims
UNION ALL
SELECT
    'holder' AS src,
    holder_name AS name,
    joined_on AS day,
    id
FROM policyholders
WHERE segment = 'VIP'
  AND LENGTH(TRIM(holder_name)) >= 7
ORDER BY src DESC
