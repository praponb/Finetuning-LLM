SELECT
    id,
    product_id,
    ROUND(unit_price / quantity, 2) AS line_total,
    ROUND(unit_price / quantity, 2) AS line_total_2
FROM order_items
ORDER BY id
LIMIT 5
