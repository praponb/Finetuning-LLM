SELECT
    id,
    order_id,
    product_id,
    quantity,
    unit_price * quantity line_total,
    CASE WHEN unit_price >= 330 THEN 'High' WHEN unit_price >= 70 THEN 'Medium' ELSE 'Low' END unit_price_tier
FROM order_items
WHERE order_id IN (4, 21) OR quantity IS NOT NULL
ORDER BY quantity, id
LIMIT 10;
