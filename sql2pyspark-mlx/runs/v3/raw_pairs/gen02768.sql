WITH order_totals AS (
    SELECT order_id, MAX(unit_price) - MIN(unit_price) AS unit_price_range, ROUND(AVG(quantity), 2) AS avg_quantity
    FROM order_items
    GROUP BY order_id
)
SELECT orders.shipping_fee, order_totals.unit_price_range, order_totals.avg_quantity
FROM orders
LEFT JOIN order_totals ON orders.id = order_totals.order_id
