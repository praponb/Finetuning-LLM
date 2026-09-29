SELECT id, carrier
FROM shipments
WHERE YEAR(delivered_date) = 2023
UNION
SELECT id, carrier
FROM shipments
WHERE YEAR(delivered_date) = 2023
