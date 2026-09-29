SELECT
    order_items.id,
    order_items.unit_price,
    orders.store_id,
    orders.customer_id,
    orders.amount
FROM order_items
LEFT JOIN orders ON order_items.order_id = orders.id
WHERE orders.discount <= 0.2 OR order_items.order_id IN (9, 20, 29)
ORDER BY store_id DESC, order_items.id DESC;
