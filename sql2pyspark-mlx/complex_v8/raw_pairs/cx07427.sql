WITH valid AS (
    -- Filter policies
    SELECT
        id,
        holder_id,
        product,
        premium,
        end_date
    FROM policies
    WHERE holder_id IS NOT NULL
)
SELECT holder_id AS ref_id, product
FROM valid
WHERE product = 'HEALTH'
  AND end_date BETWEEN DATE '2025-04-17' AND DATE '2026-09-19'
EXCEPT ALL
SELECT id, segment
FROM policyholders
WHERE risk_score > 0.2
