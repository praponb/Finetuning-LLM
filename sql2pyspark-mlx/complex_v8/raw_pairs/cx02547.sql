SELECT
    region,
    segment,
    risk_score AS metric,
    'holder' AS src
FROM policyholders
WHERE risk_score <= 0.7
UNION ALL
SELECT
    status,
    product,
    premium,
    'policy' AS src
FROM policies
ORDER BY region DESC, src
