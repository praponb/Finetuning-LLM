SELECT
    'claim' AS record_type,
    status AS category,
    claim_date,
    claim_type AS name
FROM claims
UNION ALL
SELECT
    'policy' AS record_type,
    status,
    end_date,
    product
FROM policies
WHERE end_date IS NOT NULL
  AND holder_id >= 10
ORDER BY claim_date DESC
