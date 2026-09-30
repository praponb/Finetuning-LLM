SELECT
    order_items.order_id,
    order_items.unit_price,
    orders.discount,
    customers.country
FROM order_items
JOIN orders ON orders.id = order_items.order_id
JOIN customers ON orders.customer_id = customers.id;
