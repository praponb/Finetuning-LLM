WITH OrderItemsByOrder AS (
    SELECT order_id, COUNT(quantity) AS quantity_count
    FROM order_items
    WHERE product_id IS NOT NULL
    GROUP BY order_id
)
SELECT o.discount, o.channel, O.quantity_count
FROM orders o
INNER JOIN OrderItemsByOrder O ON o.id = O.order_id
WHERE O.quantity_count >= 6;
