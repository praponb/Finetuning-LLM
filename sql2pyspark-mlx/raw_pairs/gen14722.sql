WITH OrderItemsByOrder AS (
    SELECT order_id, ROUND(STDDEV(quantity), 2) AS stddev_quantity
    FROM order_items
    GROUP BY order_id
)
SELECT o.store_id, o.shipping_fee, O.stddev_quantity
FROM orders o
LEFT OUTER JOIN OrderItemsByOrder O ON o.id = O.order_id;
