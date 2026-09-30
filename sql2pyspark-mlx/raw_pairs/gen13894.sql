SELECT shipped_date
FROM shipments
WHERE shipped_date IS NOT NULL
EXCEPT
SELECT last_restock
FROM inventory
WHERE last_restock IS NOT NULL
ORDER BY shipped_date DESC;
