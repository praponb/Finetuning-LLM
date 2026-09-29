SELECT
    id,
    product AS label,
    holder_id,
    'policy' AS origin
FROM policies
WHERE end_date BETWEEN DATE '2025-09-27' AND DATE '2027-03-20'
UNION ALL
SELECT
    id,
    product AS label,
    holder_id,
    'policy_2' AS origin
FROM policies
WHERE status IN ('ACTIVE', 'EXPIRED', 'CANCELLED')
  AND product = 'LIFE'
UNION ALL
SELECT
    holder_id AS id,
    product AS label,
    id AS holder_id,
    'policy_3' AS origin
FROM policies
