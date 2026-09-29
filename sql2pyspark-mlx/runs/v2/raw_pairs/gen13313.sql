SELECT AVG(quantity) AS avg_quantity, COUNT(*) AS cnt
FROM inventory
ORDER BY avg_quantity DESC, cnt DESC
