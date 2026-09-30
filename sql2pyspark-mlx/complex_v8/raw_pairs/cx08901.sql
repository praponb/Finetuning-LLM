SELECT product, start_date, 'policy' AS record_type
FROM policies
WHERE holder_id IS NOT NULL
  AND start_date >= DATE '2024-08-27'
UNION ALL
SELECT region, joined_on, 'holder' AS record_type
FROM policyholders
WHERE segment IN ('RETAIL', 'VIP')
UNION ALL
SELECT claim_type, claim_date, 'claim' AS record_type
FROM claims
