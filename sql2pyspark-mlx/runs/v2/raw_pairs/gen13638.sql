SELECT products.cost, t.latest_last_restock, t.avg_quantity
FROM products
JOIN (SELECT product_id, MAX(last_restock) AS latest_last_restock, ROUND(AVG(quantity), 2) AS avg_quantity
    FROM inventory
    WHERE last_restock < '2025-05-01'
    GROUP BY product_id) t ON products.id = t.product_id
WHERE t.avg_quantity > 31
ORDER BY cost DESC, t.latest_last_restock;
