SELECT p.brand, COUNT(*) AS num_inventory, AVG(i.quantity) AS avg_quantity
FROM inventory i
JOIN products p ON i.product_id = p.id
WHERE p.cost > 630
GROUP BY p.brand
ORDER BY avg_quantity DESC;
