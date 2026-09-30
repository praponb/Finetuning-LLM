SELECT
    id,
    store_id,
    quantity,
    product_id,
    PERCENT_RANK() OVER (PARTITION BY store_id ORDER BY quantity DESC) AS quantity_pct_rank,
    MAX(quantity) OVER (PARTITION BY product_id) AS product_id_max_quantity
FROM inventory
ORDER BY store_id DESC
