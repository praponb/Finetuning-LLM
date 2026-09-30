SELECT
    product_id,
    store_id,
    ROUND(STDDEV(quantity), 2) stddev_quantity,
    MAX(quantity) - MIN(quantity) quantity_range
FROM inventory
GROUP BY product_id, store_id;
