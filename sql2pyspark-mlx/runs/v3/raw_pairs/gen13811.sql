SELECT
    id,
    product_id,
    last_restock,
    quantity,
    SUM(quantity) OVER (PARTITION BY product_id ORDER BY last_restock, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_quantity
FROM inventory
WHERE last_restock >= '2023-06-24'
