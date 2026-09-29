SELECT id
FROM policyholders
WHERE risk_score <= 0.8
  AND joined_on BETWEEN DATE '2024-03-22' AND DATE '2025-10-17'
INTERSECT ALL
SELECT holder_id AS id
FROM policies
WHERE premium < 2844.1
  AND product IS NOT NULL
