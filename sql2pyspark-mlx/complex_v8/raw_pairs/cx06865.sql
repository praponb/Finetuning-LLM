SELECT 'claim' AS record_type, id AS entity_id, status
FROM claims
UNION ALL
SELECT 'holder' AS record_type, id, holder_name
FROM policyholders
