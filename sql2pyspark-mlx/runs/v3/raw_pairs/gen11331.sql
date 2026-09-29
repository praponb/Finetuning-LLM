WITH order_totals AS (
    SELECT order_id, SUM(amount) total_amount, MIN(paid_at) first_paid_at
    FROM payments
    GROUP BY order_id
)
SELECT
    orders.id,
    orders.discount,
    orders.shipping_fee,
    order_totals.total_amount,
    order_totals.first_paid_at
FROM orders
LEFT OUTER JOIN order_totals ON orders.id = order_totals.order_id
