SELECT 'holder' AS src, id AS ref_id, holder_name AS name
FROM policyholders
UNION ALL
SELECT 'holder_2' AS src, id, region
FROM policyholders
WHERE UPPER(TRIM(holder_name)) LIKE 'E%'
  AND risk_score < 0.8
ORDER BY name, ref_id
