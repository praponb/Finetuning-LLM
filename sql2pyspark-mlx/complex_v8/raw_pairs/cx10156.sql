SELECT id, start_date AS day, 'policy' AS origin
FROM policies
UNION ALL
SELECT policy_id, claim_date, 'claim' AS origin
FROM claims
WHERE approved_amount > 4554.8
