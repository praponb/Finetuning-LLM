SELECT last_restock
FROM inventory
WHERE last_restock BETWEEN '2024-12-01' AND '2025-02-01'
ORDER BY last_restock ASC
