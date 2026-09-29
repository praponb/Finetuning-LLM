SELECT products.category, SUM(inventory.quantity) AS total_quantity
FROM inventory
INNER JOIN products ON inventory.product_id = products.id
GROUP BY products.category
