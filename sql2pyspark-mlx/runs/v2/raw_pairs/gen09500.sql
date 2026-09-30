SELECT
    product_id,
    unit_price,
    quantity,
    ROUND(unit_price * 1.15, 2) AS unit_price_scaled,
    unit_price + quantity AS line_total
FROM order_items
ORDER BY unit_price DESC;
