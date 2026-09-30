SELECT
    product_id,
    store_id,
    latest_last_restock,
    quantity_count
FROM (
    SELECT
        product_id,
        store_id,
        MAX(last_restock) AS latest_last_restock,
        COUNT(quantity) AS quantity_count
    FROM inventory
    WHERE quantity BETWEEN 129 AND 175
    GROUP BY product_id, store_id
) AS t
WHERE quantity_count >= 4
