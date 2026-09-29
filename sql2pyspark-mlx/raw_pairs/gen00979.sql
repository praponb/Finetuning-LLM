SELECT
    id,
    order_id,
    unit_price,
    quantity,
    FIRST_VALUE(quantity) OVER (PARTITION BY order_id ORDER BY unit_price DESC, id) AS first_quantity
FROM order_items;
