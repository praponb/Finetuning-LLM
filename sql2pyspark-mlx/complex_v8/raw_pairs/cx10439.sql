SELECT holder_name AS label, segment, joined_on
FROM policyholders
UNION ALL
SELECT region AS label, holder_name AS segment, joined_on
FROM policyholders
WHERE region <> 'West'
UNION ALL
SELECT status AS label, claim_type AS segment, claim_date AS joined_on
FROM claims
WHERE adjuster_id >= 4
ORDER BY segment
