SELECT id, last_restock, DATEDIFF(CURRENT_DATE(), last_restock) / 365 AS years_since_last_restock
FROM inventory
WHERE quantity > 163
ORDER BY id DESC
LIMIT 50
