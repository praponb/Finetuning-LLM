SELECT
    i.last_restock,
    p.category,
    p.launch_date,
    p.price
FROM inventory i
RIGHT JOIN products p ON i.product_id = p.id
WHERE p.price < 770 AND i.quantity BETWEEN 81 AND 103
ORDER BY i.last_restock;
