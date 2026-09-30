SELECT
    id,
    status,
    order_date,
    amount,
    LEAD(amount, 1) OVER (PARTITION BY status ORDER BY order_date, id) AS next_amount
FROM orders;
