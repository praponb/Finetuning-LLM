SELECT
    id,
    quantity,
    ROUND(unit_price * 0.85, 2) adjusted_unit_price,
    quantity * unit_price line_total
FROM order_items
ORDER BY quantity DESC, adjusted_unit_price;
