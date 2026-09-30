WITH order_summary AS (
    SELECT order_id, SORT_ARRAY(COLLECT_SET(status)) AS status_list
    FROM shipments
    GROUP BY order_id
)
SELECT
    orders.id,
    orders.order_date,
    orders.channel,
    order_summary.status_list
FROM orders
JOIN order_summary ON orders.id = order_summary.order_id
ORDER BY order_date DESC, orders.id
