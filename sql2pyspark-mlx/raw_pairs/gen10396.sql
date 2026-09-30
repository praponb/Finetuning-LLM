SELECT id, status
FROM shipments
WHERE carrier IN ('DHL', 'Kerry', 'Flash')
UNION ALL
SELECT id, status
FROM shipments
WHERE cost BETWEEN 29 AND 35
