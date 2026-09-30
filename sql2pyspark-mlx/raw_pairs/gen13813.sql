SELECT
    id,
    last_restock,
    quantity,
    LEAD(quantity, 2) OVER (ORDER BY last_restock, id) next_quantity
FROM inventory
ORDER BY 3 DESC, id DESC;
