SELECT
    id,
    quantity,
    product_id,
    order_id,
    LEAST(quantity, unit_price) AS least_quantity_unit_price
FROM order_items
ORDER BY quantity DESC
