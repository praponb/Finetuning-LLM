SELECT p.category, p.brand, MAX(i.last_restock) AS latest_last_restock
FROM inventory i
LEFT JOIN products p ON i.product_id = p.id
GROUP BY p.category, p.brand
ORDER BY latest_last_restock
