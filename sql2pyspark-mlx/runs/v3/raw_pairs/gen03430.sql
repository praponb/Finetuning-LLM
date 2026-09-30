SELECT unit_price
FROM order_items
UNION
SELECT cost AS unit_price
FROM shipments
WHERE YEAR(shipped_date) = 2025
ORDER BY unit_price DESC
