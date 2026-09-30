SELECT
    id,
    product_id,
    quantity,
    order_id,
    unit_price,
    ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY quantity, id) rn,
    RANK() OVER (PARTITION BY order_id ORDER BY unit_price) unit_price_rank
FROM order_items
WHERE order_id = 6
ORDER BY id;
