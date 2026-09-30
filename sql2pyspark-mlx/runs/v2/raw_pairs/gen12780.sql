WITH order_items_by_order AS (
    SELECT order_id, MIN(unit_price) AS min_unit_price, ROUND(STDDEV(unit_price), 2) AS stddev_unit_price
    FROM order_items
    WHERE unit_price < 370
    GROUP BY order_id
)
SELECT
    orders.id,
    orders.discount,
    orders.customer_id,
    order_items_by_order.min_unit_price,
    order_items_by_order.stddev_unit_price
FROM orders
LEFT JOIN order_items_by_order ON orders.id = order_items_by_order.order_id;
