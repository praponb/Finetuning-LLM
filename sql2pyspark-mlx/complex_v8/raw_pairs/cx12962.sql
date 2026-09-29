SELECT status, claim_date AS day, claimed_amount AS metric
FROM claims
WHERE claim_date < DATE '2024-12-02'
UNION ALL
SELECT product AS status, start_date AS day, premium AS metric
FROM policies
ORDER BY day DESC, metric
