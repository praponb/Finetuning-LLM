SELECT
    id,
    order_id,
    quantity,
    product_id,
    unit_price,
    LEAD(quantity, 1) OVER (PARTITION BY order_id ORDER BY quantity, id) AS next_quantity,
    LEAD(unit_price, 1) OVER (PARTITION BY product_id ORDER BY unit_price, id) AS next_unit_price
FROM order_items;
