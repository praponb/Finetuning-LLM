SELECT
    id,
    product_id,
    last_restock,
    store_id,
    quantity,
    NTILE(10) OVER (PARTITION BY product_id ORDER BY last_restock DESC, id) AS last_restock_ntile,
    SUM(quantity) OVER (PARTITION BY store_id ORDER BY last_restock, id) AS running_quantity
FROM inventory
WHERE last_restock BETWEEN '2023-03-01' AND '2023-11-25';
