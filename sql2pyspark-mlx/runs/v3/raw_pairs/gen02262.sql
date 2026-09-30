SELECT delivered_date
FROM shipments
UNION ALL
SELECT last_restock
FROM inventory
WHERE product_id = 2
ORDER BY delivered_date DESC;
