SELECT
    id,
    customer_id,
    order_date,
    status,
    channel,
    discount,
    RANK() OVER (PARTITION BY customer_id ORDER BY order_date) AS order_date_rank,
    LEAD(discount, 2) OVER (PARTITION BY status, channel ORDER BY order_date, id) AS next_discount
FROM orders
