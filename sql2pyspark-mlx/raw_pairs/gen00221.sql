SELECT
    id,
    unit_price,
    product_id,
    order_id,
    ROUND(unit_price * 1.07, 2) AS unit_price_scaled,
    ROUND(quantity / unit_price, 2) AS line_total
FROM order_items
WHERE quantity BETWEEN 6 AND 7
ORDER BY unit_price_scaled DESC, id ASC
LIMIT 50
