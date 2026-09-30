SELECT
    id,
    product_id,
    last_restock,
    quantity,
    SUM(quantity) OVER (PARTITION BY product_id ORDER BY last_restock, id) AS running_quantity
FROM inventory
WHERE last_restock BETWEEN '2023-08-01' AND '2024-02-27'
ORDER BY id DESC;
