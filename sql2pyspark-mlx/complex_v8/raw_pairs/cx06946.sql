SELECT 'policy' AS source, holder_id, product
FROM policies
UNION ALL
SELECT 'adjuster' AS source, id AS holder_id, team AS product
FROM adjusters
