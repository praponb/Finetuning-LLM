SELECT
    id,
    status,
    order_date,
    discount,
    LEAD(discount, 2) OVER (PARTITION BY status ORDER BY order_date, id) AS next_discount
FROM orders
WHERE status = 'CANCELLED'
