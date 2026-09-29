SELECT
    id,
    product_id,
    last_restock,
    quantity,
    RANK() OVER (PARTITION BY product_id ORDER BY last_restock DESC) AS rnk,
    LEAD(quantity, 1) OVER (ORDER BY last_restock, id) AS next_quantity
FROM inventory
WHERE product_id = 4
ORDER BY rnk DESC;
