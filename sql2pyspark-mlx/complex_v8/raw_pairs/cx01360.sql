SELECT status AS label, product
FROM policies
UNION ALL
SELECT status AS label, claim_type AS product
FROM claims
UNION ALL
SELECT product AS label, status AS product
FROM policies
WHERE product <> 'TRAVEL'
ORDER BY label, product
