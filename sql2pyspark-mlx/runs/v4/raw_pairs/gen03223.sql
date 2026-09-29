SELECT *, ROUND(quantity, 1) quantity_rounded
FROM inventory
WHERE YEAR(last_restock) >= 2024 AND product_id IN (1, 8)
ORDER BY quantity DESC;
