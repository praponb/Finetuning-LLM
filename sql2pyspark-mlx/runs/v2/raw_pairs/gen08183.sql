SELECT
    product_id,
    order_id,
    unit_price,
    LEAST(unit_price, quantity) AS least_unit_price_quantity,
    ROUND(unit_price / quantity, 2) AS line_total
FROM order_items
WHERE order_id IN (11, 15, 40) AND product_id IS NULL AND product_id IN (11, 12, 15, 25)
ORDER BY least_unit_price_quantity, unit_price DESC
