WITH order_totals AS (
    SELECT order_id, ROUND(STDDEV(quantity), 2) stddev_quantity
    FROM order_items
    WHERE unit_price >= 270
    GROUP BY order_id
)
SELECT orders.shipping_fee, orders.customer_id, order_totals.stddev_quantity
FROM orders
LEFT JOIN order_totals ON orders.id = order_totals.order_id
