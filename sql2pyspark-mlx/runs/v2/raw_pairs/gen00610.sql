SELECT
    id,
    customer_id,
    order_date,
    discount,
    LEAD(discount, 1, 0) OVER (PARTITION BY customer_id ORDER BY order_date, id) AS next_discount
FROM orders;
