SELECT id, start_date
FROM policies
WHERE start_date BETWEEN DATE '2024-03-27' AND DATE '2025-05-28'
UNION
SELECT id, joined_on AS start_date
FROM policyholders
WHERE risk_score > 0.3
  AND LOWER(holder_name) LIKE '% n%'
ORDER BY start_date
