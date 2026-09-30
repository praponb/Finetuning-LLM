SELECT
    quantity,
    order_id,
    unit_price,
    unit_price + quantity AS line_total
FROM order_items
WHERE product_id IS NULL
ORDER BY line_total;
