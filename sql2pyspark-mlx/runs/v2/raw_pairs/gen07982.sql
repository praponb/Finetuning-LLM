SELECT id, segment
FROM customers
WHERE segment NOT IN ('SMB', 'Corporate', 'Consumer')
UNION ALL
SELECT id, segment
FROM customers
WHERE segment NOT IN ('SMB', 'Corporate', 'Consumer')
ORDER BY segment
