WITH filtered_policys AS (
    -- Filter policies
    SELECT
        id,
        holder_id,
        product,
        start_date,
        status
    FROM policies
    WHERE start_date >= DATE '2024-08-27'
      AND product NOT IN ('TRAVEL', 'LIFE')
)
SELECT status
FROM filtered_policys
WHERE product NOT IN ('LIFE', 'TRAVEL')
  AND start_date IS NOT NULL
EXCEPT
SELECT claim_type AS status
FROM claims
ORDER BY status
