SELECT
    id,
    quantity,
    order_id,
    unit_price,
    LEAST(quantity, unit_price) AS least_quantity_unit_price,
    ROUND(unit_price * 1.1, 2) AS unit_price_scaled
FROM order_items;
