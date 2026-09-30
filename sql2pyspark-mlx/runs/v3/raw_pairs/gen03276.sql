SELECT
    id,
    unit_price,
    order_id,
    quantity,
    ROUND(unit_price * 1.07, 2) AS adjusted_unit_price,
    COALESCE(quantity, 0) AS quantity_filled
FROM order_items
