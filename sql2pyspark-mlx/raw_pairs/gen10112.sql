SELECT
    id,
    order_date,
    discount,
    LEAD(discount, 1) OVER (ORDER BY order_date, id) AS next_discount
FROM orders
WHERE customer_id = 4
ORDER BY order_date
