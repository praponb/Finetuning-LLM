SELECT id, joined_on
FROM policyholders
UNION
SELECT id, joined_on
FROM policyholders
ORDER BY id, joined_on
