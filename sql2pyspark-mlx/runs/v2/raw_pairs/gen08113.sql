SELECT
    quantity,
    order_id,
    product_id,
    GREATEST(quantity, unit_price) AS greatest_quantity_unit_price
FROM order_items
WHERE quantity != 4;
