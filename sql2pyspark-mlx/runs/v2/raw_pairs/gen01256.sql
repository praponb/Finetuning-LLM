SELECT
    order_items.unit_price,
    orders.order_date,
    orders.status,
    ADD_MONTHS(orders.order_date, 12) AS order_date_plus_months
FROM order_items
LEFT JOIN orders ON orders.id = order_items.order_id
WHERE orders.shipping_fee <= 20 AND order_items.product_id IN (8, 15, 26, 29);
