WITH order_stats AS (
    SELECT order_id, MAX(shipped_date) AS last_shipped_date
    FROM shipments
    WHERE cost < 65
    GROUP BY order_id
)
SELECT o.shipping_fee, o.status, os.last_shipped_date
FROM orders o
LEFT JOIN order_stats os ON o.id = os.order_id;
