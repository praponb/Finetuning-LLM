SELECT
    YEAR(last_restock) AS last_restock_year,
    product_id,
    store_id,
    MIN(last_restock) AS first_last_restock,
    MAX(quantity) AS max_quantity
FROM inventory
WHERE quantity <> 176
GROUP BY last_restock_year, product_id, store_id
