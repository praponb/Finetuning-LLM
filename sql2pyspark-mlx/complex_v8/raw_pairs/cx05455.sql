SELECT claim_type AS name, reported_at, 'claim' AS src
FROM claims
WHERE approved_amount < 23120.7
UNION
SELECT claim_type, reported_at, 'claim_2' AS src
FROM claims
WHERE claimed_amount < 29985.9
