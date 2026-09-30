SELECT p.id, COUNT(i.id) AS inventory_count, COUNT(DISTINCT i.quantity) AS unique_stores
FROM products p
LEFT OUTER JOIN inventory i ON i.product_id = p.id
GROUP BY p.id
ORDER BY unique_stores DESC, inventory_count DESC, id
LIMIT 20;
