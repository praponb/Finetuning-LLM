SELECT
    'holder' AS record_type,
    segment AS category,
    risk_score,
    joined_on AS event_date
FROM policyholders
UNION ALL
SELECT
    'policy' AS record_type,
    product,
    premium,
    start_date
FROM policies
WHERE holder_id <= 5
UNION ALL
SELECT
    'holder_3' AS record_type,
    region,
    risk_score,
    joined_on
FROM policyholders
WHERE region NOT IN ('Central', 'North')
