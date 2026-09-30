SELECT
    id AS entity_id,
    holder_id,
    status AS category,
    'policy' AS src
FROM policies
WHERE product IS NOT NULL
  AND status NOT IN ('LAPSED', 'CANCELLED')
UNION ALL
SELECT
    id,
    holder_id,
    status,
    'policy_2' AS src
FROM policies
WHERE premium <= 4670.9
UNION ALL
SELECT
    id,
    level,
    adjuster_name,
    'adjuster' AS src
FROM adjusters
WHERE LOWER(adjuster_name) LIKE '% s%'
  AND team = 'Health'
ORDER BY src DESC
