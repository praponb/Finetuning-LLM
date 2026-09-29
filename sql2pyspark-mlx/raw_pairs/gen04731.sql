SELECT last_restock
FROM inventory
WHERE YEAR(last_restock) = 2025 AND quantity != 168;
