SELECT products.cost, inventory.quantity
FROM products
LEFT JOIN inventory ON products.id = inventory.product_id
WHERE inventory.store_id IN (2, 5) AND inventory.quantity BETWEEN 141 AND 195
ORDER BY cost;
