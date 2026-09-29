SELECT p.cost, t.first_last_restock, t.last_last_restock
FROM products p
LEFT JOIN (SELECT product_id, MIN(last_restock) AS first_last_restock, MAX(last_restock) AS last_last_restock
    FROM inventory
    WHERE store_id IN (4, 5, 6)
    GROUP BY product_id) t ON p.id = t.product_id
ORDER BY first_last_restock, p.cost DESC
