SELECT cost
FROM shipments
WHERE carrier = 'Kerry'
UNION ALL
SELECT price
FROM products
ORDER BY cost DESC
