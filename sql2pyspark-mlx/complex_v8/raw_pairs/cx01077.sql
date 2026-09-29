SELECT id AS key_id, region, joined_on AS event_date
FROM policyholders
UNION ALL
SELECT id AS key_id, status AS region, claim_date AS event_date
FROM claims
