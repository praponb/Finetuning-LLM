SELECT id, status AS category
FROM policies
WHERE status = 'LAPSED'
MINUS
SELECT id, region
FROM policyholders
