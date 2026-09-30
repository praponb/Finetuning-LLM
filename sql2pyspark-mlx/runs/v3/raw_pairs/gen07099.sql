SELECT
    id,
    quantity,
    product_id,
    unit_price,
    DENSE_RANK() OVER (ORDER BY quantity DESC) AS quantity_dense_rank,
    NTILE(3) OVER (PARTITION BY product_id ORDER BY unit_price DESC, id) AS bucket
FROM order_items
