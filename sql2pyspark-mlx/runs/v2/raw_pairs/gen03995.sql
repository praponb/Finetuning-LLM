SELECT
    orders.id,
    orders.discount,
    orders.order_date,
    orders.customer_id
FROM orders
WHERE orders.amount < 1370 AND NOT EXISTS (SELECT 1 FROM order_items WHERE order_items.order_id = orders.id AND order_items.quantity BETWEEN 3 AND 5)
ORDER BY 1 DESC
