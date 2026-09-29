SELECT claim_type AS category, status AS category_2, claimed_amount
FROM claims
UNION
SELECT status, product, premium
FROM policies
WHERE status IS NOT NULL
ORDER BY claimed_amount, category_2
