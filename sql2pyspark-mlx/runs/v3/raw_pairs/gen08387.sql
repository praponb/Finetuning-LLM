WITH order_items_by_order AS (
    SELECT order_id, COUNT(DISTINCT quantity) AS unique_products
    FROM order_items
    WHERE product_id IN (13, 15, 34, 40)
    GROUP BY order_id
)
SELECT
    orders.id,
    orders.customer_id,
    orders.shipping_fee,
    order_items_by_order.unique_products
FROM orders
LEFT OUTER JOIN order_items_by_order ON orders.id = order_items_by_order.order_id
WHERE order_items_by_order.unique_products > 9
ORDER BY id DESC;
