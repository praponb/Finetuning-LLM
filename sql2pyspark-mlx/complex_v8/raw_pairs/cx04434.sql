SELECT id
FROM policyholders
WHERE joined_on < DATE '2025-07-02'
MINUS
SELECT id
FROM policyholders
