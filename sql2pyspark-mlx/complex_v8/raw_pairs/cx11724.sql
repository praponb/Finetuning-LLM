SELECT id AS key_id, start_date, status
FROM policies
WHERE holder_id IS NOT NULL
UNION ALL
SELECT id, joined_on, holder_name
FROM policyholders
UNION ALL
SELECT id, start_date, product
FROM policies
WHERE holder_id IS NOT NULL
