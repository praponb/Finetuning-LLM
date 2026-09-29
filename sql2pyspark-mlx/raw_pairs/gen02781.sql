SELECT *, COALESCE(quantity, 0) AS quantity_filled, ROUND(quantity, 1) AS quantity_rounded
FROM inventory
WHERE (quantity BETWEEN 182 AND 222 AND last_restock <= '2025-02-03') OR quantity <= 109
ORDER BY last_restock, id
LIMIT 50
