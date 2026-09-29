SELECT
    order_id,
    unit_price,
    quantity + unit_price AS line_total,
    ROUND(quantity * 1.1, 2) AS quantity_scaled
FROM order_items
ORDER BY line_total, unit_price;
