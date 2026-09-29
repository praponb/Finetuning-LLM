SELECT
    store_id,
    product_id,
    COUNT(*) AS num_inventory,
    MAX(quantity) AS max_quantity
FROM inventory
GROUP BY store_id, product_id;
