SELECT
    id,
    order_id,
    unit_price - quantity AS line_total,
    quantity - unit_price AS line_total_2
FROM order_items
ORDER BY id, 3
LIMIT 1;
