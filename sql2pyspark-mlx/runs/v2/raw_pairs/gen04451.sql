SELECT
    id,
    unit_price,
    order_id,
    product_id,
    ROUND(unit_price / quantity, 2) AS line_total,
    unit_price + quantity AS line_total_2
FROM order_items
WHERE (order_id IN (12, 24) AND order_id IN (6, 21)) OR unit_price <= 210
ORDER BY id
