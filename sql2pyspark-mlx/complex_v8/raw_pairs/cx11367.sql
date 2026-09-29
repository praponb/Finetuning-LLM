WITH valid_holders AS (
    -- Scope the policyholders rows
    SELECT *
    FROM policyholders
    WHERE holder_name IS NOT NULL
)
SELECT 'holder' AS src, id AS key_id, region AS name
FROM valid_holders
UNION ALL
SELECT 'holder_2' AS src, id, segment
FROM policyholders
WHERE UPPER(TRIM(holder_name)) LIKE 'A%'
  AND segment = 'VIP'
ORDER BY src, name
