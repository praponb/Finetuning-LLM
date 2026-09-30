SELECT
    id,
    unit_price,
    product_id,
    unit_price + quantity AS line_total
FROM order_items
WHERE unit_price >= 120 AND order_id = 24
ORDER BY unit_price DESC
