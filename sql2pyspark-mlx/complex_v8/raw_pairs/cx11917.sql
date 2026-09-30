WITH filtered AS (
    -- Scope the claims rows
    SELECT *
    FROM claims
    WHERE status <> 'IN_REVIEW'
      AND claim_type IN ('collision', 'fire', 'theft')
)
SELECT claim_type AS name, status, 'claim' AS src
FROM filtered
UNION ALL
SELECT status, product, 'policy' AS src
FROM policies
WHERE start_date >= DATE '2024-08-27'
UNION ALL
SELECT holder_name, region, 'holder' AS src
FROM policyholders
WHERE region = 'Central'
  AND risk_score > 0.3
ORDER BY status
