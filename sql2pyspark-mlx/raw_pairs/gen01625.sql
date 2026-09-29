WITH OrderStats AS (
    SELECT order_id, COUNT(DISTINCT unit_price) AS unique_orders
    FROM order_items
    GROUP BY order_id
)
SELECT
    o.id,
    o.shipping_fee,
    o.store_id,
    O.unique_orders
FROM orders o
LEFT OUTER JOIN OrderStats O ON o.id = O.order_id;
