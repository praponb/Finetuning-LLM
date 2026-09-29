SELECT
    store_id,
    product_id,
    quantity,
    ROUND(quantity * 100, 2) AS adjusted_quantity
FROM inventory
WHERE last_restock BETWEEN '2023-08-10' AND '2024-10-01' AND quantity <= 111
ORDER BY 3, product_id;
