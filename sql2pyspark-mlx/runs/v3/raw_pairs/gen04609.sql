SELECT
    id,
    order_date,
    amount,
    LEAD(amount, 1) OVER (ORDER BY order_date, id) AS next_amount
FROM orders
