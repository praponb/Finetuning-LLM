WITH order_totals AS (
    SELECT order_id, MIN(shipped_date) AS first_shipped_date, MIN(cost) AS min_cost
    FROM shipments
    GROUP BY order_id
)
SELECT
    orders.id,
    orders.order_date,
    orders.shipping_fee,
    order_totals.first_shipped_date,
    order_totals.min_cost
FROM orders
INNER JOIN order_totals ON orders.id = order_totals.order_id
WHERE order_totals.min_cost > 70
ORDER BY id DESC, min_cost;
