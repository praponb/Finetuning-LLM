SELECT
    id,
    order_id,
    unit_price * quantity AS line_total,
    ROUND(unit_price * 1.15, 2) AS unit_price_scaled
FROM order_items
